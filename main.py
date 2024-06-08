from mcstatus import JavaServer
import sqlite3
from datetime import datetime
import argparse
from time import sleep


class Users:
    def __init__(self, id, username, creationdate):
        self.ID = id
        self.Username = username
        self.CreationDate = creationdate


class UserInfo:
    def __init__(self, userid, allonlinetime, themostonlinetimesinarow, lastonline):
        self.UserID = userid
        self.AllOnlineTime = allonlinetime
        self.TheMostOnlineTimesInARow = themostonlinetimesinarow
        self.LastOnline = lastonline


class SQL:
    def __init__(self, server_name):
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

    def select(self, table_name: str, **kwargs: str):
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

    def insert(self, table_name: str, **kwargs: str):
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

    def update(self, table_name: str, **kwargs):
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
    def __init__(self):
        self.log_name = datetime.now().strftime("%Y-%m-%d") + '.txt'
        now = datetime.now()
        fd = open(self.log_name, 'a')
        fd.write(now.strftime(
            "[%Y-%m-%d]-[%H:%M:%S]:: ") + 'Starting executing.                                                                                                             ..' + '\n')
        fd.close()

    def get_date(self):
        now = datetime.now()
        return now.strftime("[%Y-%m-%d]-[%H:%M:%S]:: ")

    def log_add_info(self, message):
        message += '\n'
        fd = open(self.log_name, 'a')
        fd.write(self.get_date() + message)
        fd.close()


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
    system = {}
    server = JavaServer.lookup(args.server_address, args.port)
    while True:
        sleep(args.time * 60)
        info = server.status()
        if server.ping() > 100:
            log.log_add_info('Server timeout > 100s')
            sleep(180)
            server = JavaServer.lookup(args.server_address, args.port)

        if info.players.sample is None:
            log.log_add_info('Server is empty')
            sleep(180)
            continue

        for player in info.players.sample:
            if not player.name in system:
                system[player.name] = 0
            system[player.name] += 1
            user = sql.select('Users', Username=player.name)
            if user:
                log.log_add_info(f'Update time for {player.name}')
                _temp = sql.select('UserInfo', UserID=user.ID)
                if system[player.name] >= _temp.TheMostOnlineTimesInARow:
                    log.log_add_info(f'New record time for {player.name} is {system[player.name]}')
                    sql.update('UserInfo'
                               ,SAllOnlineTime=_temp.AllOnlineTime + args.time
                               ,STheMostOnlineTimesInARow=system[player.name]
                               ,CUserID=_temp.UserID)
                elif system[player.name] < _temp.TheMostOnlineTimesInARow:
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