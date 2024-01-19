import os
import platform
from sqlite3 import Cursor
from typing import List, Any

import database.class_conn
import class_exception


class C_DBase:
    def __init__(self):
        self.DataBaseConn = database.class_conn.C_DBaseConn()  # Criando a instância do Banco de Dados

# Métodos Novos
    def getSE_AT_DB(self):
        # Seleciona e salva as subestações presentes nos CTMT numa lista em ordem alfabética
        try:
            subs = self.DataBaseConn.getSQLDB("CTMT", "SELECT DISTINCT sub FROM ctmt ORDER BY sub")

            lista_de_subestacoes_de_alta_tensao = [sub[0] for sub in subs.fetchall()]

            return lista_de_subestacoes_de_alta_tensao
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Circuitos de Alta Tensão!")

    def getCirAT_MT_DB(self, nomeSE_AT):
        # Seleciona e salva os CTAT de uma determinada SUB numa lista em ordem alfabética e sem sufixos numéricos
        try:
            ctats = self.DataBaseConn.getSQLDB("CTAT", "SELECT DISTINCT nome FROM ctat WHERE nome LIKE '" +
                                               nomeSE_AT + "%' AND NOT (nome LIKE '%2' OR nome LIKE '%3' OR nome LIKE \
                                               '%4') ORDER BY nome")


            lista_de_circuitos_de_alta_para_media = [ctat[0] for ctat in ctats.fetchall()]

            return lista_de_circuitos_de_alta_para_media
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Circuitos de Média Tensão!")

    def getSE_MT_AL_DB(self, nomeSE_MT):
        # Seleciona e salva os CTMT de uma determinada SUB numa lista em ordem alfabética
        try:
            ctmts = self.DataBaseConn.getSQLDB("CTMT", "SELECT DISTINCT nome, cod_id FROM ctmt WHERE sub\
                                               = '" + nomeSE_MT[0]+"'ORDER BY nome")  # OBS: nomeSE_MT é uma lista

            lista_de_alimentadores_de_media_tensao = [ctmt for ctmt in ctmts.fetchall()]

            return lista_de_alimentadores_de_media_tensao
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Alimentadores de Média Tensão!")

    def getSE_MT_AL_TrafoDIST(self, codField):
        # Seleciona e salva os tranformadores de distribuição presentes num CTMT numa lista
        try:
            untrds = self.DataBaseConn.getSQLDB("UNTRMT", "SELECT DISTINCT cod_id, pot_nom FROM  untrmt\
                                                WHERE ctmt = '" + codField + "' AND pos = 'PD' ORDER BY cod_id")

            lista_transformadores_de_distribuicao = [untrd[0] for untrd in untrds]

            return lista_transformadores_de_distribuicao
        except:
            raise class_exception.ExecData("Erro no processamento do Banco de Dados para os Transformadores de\
                                           Distribuição!")
