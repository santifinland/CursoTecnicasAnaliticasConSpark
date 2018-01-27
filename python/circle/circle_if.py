# -*- coding: utf-8 -*-
# Programa de cálculo de circunferencia de un círculo

from math import pi


def calcula_circunferencia(r):
    return 2 * pi * float(r)


def is_numeric(x):
    try:
        float(x)
        return True
    except:
        return False


print("Programa de cálculo de la circunferencia de un círculo dado su radio")
radio = raw_input("¿Radio del círculo? ")
if is_numeric(radio):  # if <boolean>:
    circunferencia = calcula_circunferencia(radio)
    print("Circunferencia para el círculo de radio {}: {}\n".format(radio, circunferencia))
else:
    print("Ese radio no es un número")
