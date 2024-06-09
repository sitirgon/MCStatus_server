from mcstatus import JavaServer
import sqlite3
from datetime import datetime
import argparse
from time import sleep


class Users:
    def __init__(self, id, username, creationdate) -> None:
        self.ID = id
        self.Username = username
        self.CreationDate = creationdate


class UserInfo:
    def __init__(self, userid, allonlinetime, themostonlinetimesinarow, lastonline) -> None:
        self.UserID = userid
        self.AllOnlineTime = allonlinetime
        self.TheMostOnlineTimesInARow = themostonlinetimesinarow
        self.LastOnline = lastonline


class SQL:
    def __init__(self, server_name) -> None:
        self.con = sqlite3.connect(server_name + '.db')
        self.cur = self.con.cursor()
        try:
            # base config
            self.cur.execute('''CREATE TABLE Users(
                ID INTEGER PRIMARY KEY,
                Username VARCHAR(250) NOT NULL,
                CreationDate DATETIME DEFAULT (datetime('now', 'localtime')) NOT NULL)''')
            self.cur.execute('''CREATE TABLE UserInfo (
                UserID INTEGER NOT NULL,
                AllOnlineTime INTEGER NOT NULL,
                TheMostOnlineTimesInARow INTEGER NOT NULL,
                LastOnline DATETIME DEFAULT (datetime('now', 'localtime')) NOT NULL,
                FOREIGN KEY (UserID) REFERENCES Users(id))''')
            self.cur.execute('''CREATE TRIGGER dateUpdate AFTER UPDATE ON UserInfo
                 BEGIN
                  update UserInfo SET LastOnline = datetime('now', 'localtime') WHERE UserID = NEW.UserID;
                 END;''')
            self.con.commit()
        except sqlite3.OperationalError:
            pass

    def select(self, table_name: str, **kwargs: str) -> object:
        argv = [key + '=' + "'" + str(value) + "'" for key, value in kwargs.items()]
        conditional = ' AND '.join(argv)
        if argv:
            sql = f'SELECT * FROM {table_name} WHERE {conditional}'
        elif not argv:
            sql = f'SELECT * FROM {table_name}'
        result = self.cur.execute(sql).fetchone()
        if not result:
            return result
        if table_name == 'Users':
            return Users(result[0], result[1], result[2])
        elif table_name == 'UserInfo':
            return UserInfo(result[0], result[1], result[2], result[3])

    def insert(self, table_name: str, **kwargs: str) -> int:
        columns = []
        values = []
        for key, value in kwargs.items():
            columns.append(str(key))
            values.append("'" + str(value) + "'")
        columns = ', '.join(columns)
        values = ', '.join(values)
        sql = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'
        self.cur.execute(sql)
        self.con.commit()
        return 1

    def update(self, table_name: str, **kwargs) -> int:
        argv = [key + '=' + "'" + str(value) + "'" for key, value in kwargs.items()]
        conditional = [i[1:] for i in argv if i[0] == 'C']
        set_update = [i[1:] for i in argv if i[0] == 'S']

        set_update = ', '.join(str(i) for i in set_update)
        conditional = ' AND '.join(str(i) for i in conditional)

        sql = f'UPDATE {table_name} SET {set_update} WHERE {conditional}'
        self.cur.execute(sql)
        self.con.commit()
        return 1


class Logger:
    def __init__(self) -> None:
        self.log_name = datetime.now().strftime("%Y-%m-%d") + '.txt'
        now = datetime.now()
        fd = open(self.log_name, 'a')
        fd.write(now.strftime(
            "[%Y-%m-%d]-[%H:%M:%S]:: ") + 'Starting executing.                                                                                                             ..' + '\n')
        fd.close()

    def get_date(self):
        now = datetime.now()
        return now.strftime("[%Y-%m-%d]-[%H:%M:%S]:: ")

    def log_add_info(self, message) -> None:
        message += '\n'
        fd = open(self.log_name, 'a')
        fd.write(self.get_date() + message)
        fd.close()

    def check_date(self) -> None:
        _temp = datetime.now().strftime("%Y-%m-%d") + '.txt'
        if _temp > self.log_name:
            self.log_name = _temp


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get info from a minecraft server', allow_abbrev=False)
    parser.add_argument('-sa', '--server_address', metavar='server_adders',
                        required=True, action='store', type=str)
    parser.add_argument('-p', '--port', metavar='port_server',
                        required=True, action='store', type=int)
    parser.add_argument('-t', '--time', metavar='time loop in minute',
                        required=True, action='store', type=int)
    args = parser.parse_args()

    sql = SQL(args.server_address)
    log = Logger()
    global_system = {}
    server = JavaServer.lookup(args.server_address, args.port)
    while True:
        log.check_date()

        temp_system = global_system.copy()
        global_system.clear()

        sleep(args.time * 60)
        try:
            info = server.status()
        except ConnectionRefusedError:
            log.log_add_info("Server status failed 'ConnectionRefusedError', I am starting second try")
            server = JavaServer.lookup(args.server_address, args.port)
            info = server.status()

        try:
            ping = server.ping()
        except ConnectionRefusedError:
            log.log_add_info("Server ping failed 'ConnectionRefusedError', I am starting second try")
            server = JavaServer.lookup(args.server_address, args.port)
            ping = server.ping()

        if ping > 200:
            log.log_add_info('Server timeout > 100s')
            sleep(180)
            server = JavaServer.lookup(args.server_address, args.port)

        if info.players.sample is None:
            log.log_add_info('Server is empty')
            sleep(180)
            continue

        for player in info.players.sample:
            if player.name in temp_system:
                global_system[player.name] = temp_system[player.name]
            if player.name not in temp_system:
                global_system[player.name] = 0

        for player in info.players.sample:
            global_system[player.name] += 1
            user = sql.select('Users', Username=player.name)
            if user:
                log.log_add_info(f'Update time for {player.name}')
                _temp = sql.select('UserInfo', UserID=user.ID)
                if global_system[player.name] > _temp.TheMostOnlineTimesInARow:
                    log.log_add_info(f'New record time for {player.name} is {global_system[player.name]}')
                    sql.update('UserInfo'
                               ,SAllOnlineTime=_temp.AllOnlineTime + args.time
                               ,STheMostOnlineTimesInARow=global_system[player.name]
                               ,CUserID=_temp.UserID)
                elif global_system[player.name] <= _temp.TheMostOnlineTimesInARow:
                    sql.update('UserInfo'
                           ,SAllOnlineTime=_temp.AllOnlineTime+args.time
                           ,CUserID=_temp.UserID)
            elif not user:
                log.log_add_info(f'New Account {player.name}')
                sql.insert('Users', Username=player.name)
                user_id = sql.select('Users', Username=player.name)
                sql.insert('UserInfo',
                           UserID=user_id.ID,
                           AllOnlineTime=args.time,
                           TheMostOnlineTimesInARow=args.time)
