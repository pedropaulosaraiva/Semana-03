from typing import List, Any

import database.class_conn
import class_exception
import prodist.tpos
import prodist.ttranf
import prodist.tpostotran

class C_DBaseCoord():
    def __init__(self):
        self._DataBaseConn = ""
        self.DataBaseConn = database.class_conn.C_DBaseConn()

    @property
    def DataBaseConn(self):
        return self._DataBaseConn

    @DataBaseConn.setter
    def DataBaseConn(self, value):
        self._DataBaseConn = value

    ######################## Visualização

    def getCods_AL_SE_MT_DB(self, listaNomesAL_MT):
        #Pega os códigos dos alimenatadores de uma SE MT
        try:
            ctmts = self.DataBaseConn.getSQLDB("CTMT", "SELECT DISTINCT cod_id FROM ctmt WHERE nom IN(" +
                                               str(listaNomesAL_MT)[1:-1] + ") ORDER BY nom")

            lista_de_identificadores_dos_alimentadores = [ctmt[0] for ctmt in ctmts.fetchall()]

            return lista_de_identificadores_dos_alimentadores
        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar os Códigos dos Alimentadores de Média Tensão!")

    def getCoord_AL_SE_MT_DB(self, nomeAL_MT): #Pega as coordenadas de um alimentador de uma SE MT

        try:
            nomeAL_MTS = []
            nomeAL_MTS.append(str(nomeAL_MT))
            codAlimentador = self.getCods_AL_SE_MT_DB(nomeAL_MTS)


            cod_al = self.DataBaseConn.getSQLDB("SSDMT", "SELECT DISTINCT ctmt,x,y,vertex_index,pac_1,objectid FROM\
                                                ssdmt WHERE ctmt ='" + str(codAlimentador[0]) + "' ORDER BY objectid")

            lista_de_coordenadas_do_alimentador: list[list[list[int]]] = []
            lista_de_dados_linha: list[str] = []
            for linha in cod_al.fetchall():
                    if linha[3] == 0:
                        dadosCoordInicio = [linha[2], linha[1]]
                        continue
                    if linha[3] == 1:
                        dadosCoordFim = [linha[2], linha[1]]

                    dadosCoord: list[list[int]] = [dadosCoordInicio, dadosCoordFim]
                    lista_de_coordenadas_do_alimentador.append(dadosCoord)

                    dadoslinha = str(linha[4])
                    lista_de_dados_linha.append(dadoslinha)

            return lista_de_coordenadas_do_alimentador, lista_de_dados_linha

        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar as Coordenadas dos Alimentadores de Média Tensão!")

    def getCoord_AL_SE_MT_BT_DB(self, codTD): #Pega as coordenadas dos circuitos de baixa de um alimentador

        try:
            cod_al = self.DataBaseConn.getSQLDB("SSDBT", "SELECT DISTINCT ctmt,x,y,vertex_index,objectid FROM ssdbt\
                                                WHERE uni_tr_d ='" + codTD + "' ORDER BY objectid")

            lista_de_coordenadas_BT: list[list[list[int]]] = []
            for linha in cod_al.fetchall():
                    if linha[3] == 0:
                        dadosCoordInicio = [linha[2], linha[1]]
                        continue
                    if linha[3] == 1:
                        dadosCoordFim = [linha[2], linha[1]]

                    dadosCoord = [dadosCoordInicio, dadosCoordFim]
                    lista_de_coordenadas_BT.append(dadosCoord)

            return lista_de_coordenadas_BT

        except:
            raise class_exception.ExecDataBaseError("Erro ao pegar as Coordenadas dos Alimentadores de Baixa Tensão!")


    def getData_TrafoDIST(self, nomeSE_MT, codField):  # Pega os reguladores de MT

        try:
            dadosUNTRD = self.DataBaseConn.getSQLDB("UNTRD", "SELECT cod_id, pot_nom, ctmt, x, y, tip_trafo, pos,\
                                                    posto, pac_1, pac_2 FROM  untrd WHERE sub = '" + nomeSE_MT[0] +
                                                    "' AND ctmt = '" + codField + "'")

            lista_dados = [[lnhUNTRD[4],lnhUNTRD[3],lnhUNTRD[0],lnhUNTRD[1],prodist.ttranf.TTRANF[lnhUNTRD[5]],
                           prodist.tpos.TPOS[lnhUNTRD[6]], prodist.tpostotran.TPOSTOTRAN[lnhUNTRD[7]],lnhUNTRD[8],
                           lnhUNTRD[9]] for lnhUNTRD in dadosUNTRD.fetchall()]

            return lista_dados

        except:
            raise class_exception.ExecData(
                "Erro no processamento do Banco de Dados para os Transformadores de Distribuição! ")


    def getData_UniConsumidoraMT(self, nomeSE_MT, codField):
        try:
            dadosSSDMT = self.DataBaseConn.getSQLDB("SSDMT", "SELECT pn_con_1, pn_con_2, x, y FROM  ssdmt WHERE\
                                                    sub = '" + nomeSE_MT[0] + "' AND ctmt = '" + codField + "'")

            tmp_ssdmt = [[lnhSSDMT[0],lnhSSDMT[1],lnhSSDMT[2],lnhSSDMT[3]] for lnhSSDMT in dadosSSDMT.fetchall()]

            dadosUCMT = self.DataBaseConn.getSQLDB("UCMT", "SELECT pn_con, brr, sit_ativ, car_inst, dat_con, ctmt FROM\
                                                   ucmt WHERE sub = '" +nomeSE_MT[0]+ "' AND ctmt = '" +codField+ "'")

            lista_dados = []
            for lnhUCMT in dadosUCMT.fetchall():  # Pegando o Transformador

                tmp_dados = []
                for lnhSSDMT in tmp_ssdmt:

                    if (lnhSSDMT[0] == lnhUCMT[0]) or (lnhSSDMT[1] == lnhUCMT[0]):

                        tmp_dados = [lnhSSDMT[3], lnhSSDMT[2], lnhUCMT[1],lnhUCMT[2],lnhUCMT[3],lnhUCMT[4]]

                        break
                lista_dados.append(tmp_dados)


            return lista_dados

        except:
            raise class_exception.ExecData(
                "Erro no processamento do Banco de Dados para as Unidades Consumidoras de Média Tensão! ")
