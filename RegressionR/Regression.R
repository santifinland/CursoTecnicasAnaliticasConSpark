# Regression
library(ggplot2)
library(plyr)
elections <- read.csv("/Users/sdmt/Personal/Esic/Esic/data/EleccionesMadrid2016.csv")
catastro <- read.csv("/Users/sdmt/Personal/Esic/Esic/data/CatastroMadrid2014.csv")
total = elections$PP + elections$PodemosIU + elections$PSOE + elections$Ciudadanos +
    elections$Pacma + elections$Vox + elections$UPyD + elections$RecortesCero +
    elections$FEJONS + elections$PCPE + elections$PH + elections$SAIn +
    elections$PLIB + elections$Abstencion + elections$Nulo + elections$Blanco
df <- join(elections, catastro, by="Distrito")
df["pp_percentage"] = elections$PP / total
reg <- lm(df$pp_percentage ~ df$ValorMedio)

g <- ggplot(df, aes(ValorMedio, pp_percentage, label=Distrito)) +
    geom_text() +
    xlim(0, 200) + ylim(0, 0.5) +
    xlab("Valor medio Catastro") + ylab("Porcentage votos PP") +
    geom_abline(intercept=reg$coefficients[1], slope=reg$coefficients[2]) +
    theme_bw()
print(g)

g <- ggplot(df, aes(ValorMedio, pp_percentage, label=Distrito)) +
    geom_point() +
    xlim(0, 200) + ylim(0, 0.5) +
    xlab("Valor medio Catastro") + ylab("Porcentage votos PP") +
    geom_abline(intercept=reg$coefficients[1], slope=reg$coefficients[2]) +
    theme_bw()
print(g)