import class_exception

import sqlite3
import MySQLdb


class C_DBaseConn:  # Classe de banco de dados

    def __init__(self):
        # Variáveis das Classes

        # Informações do Diretório
        self._DataBaseInfo = {}

    @property
    def DataBaseInfo(self):
        # Este código é executado quando alguém for
        # ler o valor de self.nome
        return self._DataBaseInfo

    @DataBaseInfo.setter
    def DataBaseInfo(self, nDataBaseInfo):
        self._DataBaseInfo = nDataBaseInfo

    def getSQLDB(self, nomeBancoDados, strSQL):

        try:
            # Conectando em apenas leitura!
            if self.DataBaseInfo["Conn"] == "sqlite":

                connDB = sqlite3.connect(
                    'file:' + self.DataBaseInfo["Sqlite_DirDataBase"] + nomeBancoDados + '.sqlite?mode=ro', uri=True)

                cbanco = connDB.execute(strSQL)

            elif self.DataBaseInfo["Conn"] == "mysql":

                connDB = MySQLdb.connect(self.DataBaseInfo['MySQL_Host'], self.DataBaseInfo['MySQL_User'],
                                         self.DataBaseInfo['MySQL_Passwd'], self.DataBaseInfo['MySQL_db'])

                cbanco = connDB.cursor()
                cbanco.execute(strSQL)

                connDB.close()

            return cbanco

        except:
            raise class_exception.ConnDataBaseError("Erro de conexão no Banco de Dados:" + nomeBancoDados)

    def testConn(self):

        try:
            # Conectando em apenas leitura!
            if self.DataBaseInfo["Conn"] == "sqlite":

                connDB = sqlite3.connect('file:' + self.DataBaseInfo["Sqlite_DirDataBase"] + 'CTAT.sqlite?mode=ro',
                                         uri=True)

                cbanco = connDB.execute('select sqlite_version();')

            elif self.DataBaseInfo["Conn"] == "mysql":

                connDB = MySQLdb.connect(self.DataBaseInfo['MySQL_Host'], self.DataBaseInfo['MySQL_User'],
                                         self.DataBaseInfo['MySQL_Passwd'], self.DataBaseInfo['MySQL_db'])

                cursor = connDB.cursor()
                cursor.execute('SELECT VERSION();')
                results = cursor.fetchone()

                connDB.close()

                if not results:
                    return False

            return True

        except:
            return False
