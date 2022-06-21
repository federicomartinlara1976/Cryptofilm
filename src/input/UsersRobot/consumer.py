#!/usr/bin/env python

import sys

sys.path.append('/home/federico/git/Cryptofilm/src')

import funcionesRobot as fr

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Se queda en bucle esperando por mensajes
    fr.insertUsersFromKafka()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
