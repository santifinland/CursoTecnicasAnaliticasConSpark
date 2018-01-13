# -*- coding: utf-8 -*-
# Programa de cálculo de circunferencia de un círculo

from math import pi


def calcula_circunferencia(r):  # Definición de una función con parámetros
    return 2 * pi * r           # Uso de sentencia return


print("Programa de cálculo de la circunferencia de un círculo dado su radio")
radio = input("¿Radio del círculo? ")
circunferencia = calcula_circunferencia(radio)  # Llamada a función con parámetros
print("Circunferencia para el círculo de radio {}: {}\n".format(radio, circunferencia))
