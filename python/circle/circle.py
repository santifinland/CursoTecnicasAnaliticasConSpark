# -*- coding: utf-8 -*-
# Programa de cálculo de circunferencia de un círculo

from math import pi

print("Programa de cálculo de la circunferencia de un círculo dado su radio")

radio = input("¿Radio del círculo? ")
circunferencia = 2 * pi * radio

print("Circunferencia para el círculo de radio {}: {}\n".format(radio, circunferencia))
