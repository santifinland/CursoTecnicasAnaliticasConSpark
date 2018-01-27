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
radio = float(raw_input("¿Radio del círculo? "))
print(radio)
if is_numeric(radio) == True:
    for i in range(0, int(float(radio))):  # for <variable> in <sequence>:
        circunferencia = calcula_circunferencia(i)
        print("Circunferencia para el círculo de radio {}: {}".format(i, circunferencia))
else:
    print("Ese radio no es un número")
