# Logistic regression

hours <- c(0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 1.75, 2.00, 2.25, 2.50,
           2.75, 3.00, 3.25, 3.50, 4.00, 4.25, 4.50, 4.7, 5.00, 5.50)
pass <- c(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1)

df <- data.frame(hours, pass)
reg <- lm(df$pass~ df$hours)

g <- ggplot(df, aes(hours, pass)) +
     geom_point() +
     #xlim(0, 200) + ylim(0, 0.5) +
     xlab("Horas de estudio") + ylab("Aprobado") +
     geom_abline(intercept=reg$coefficients[1], slope=reg$coefficients[2]) +
     theme_bw()
print(g)

reglogit <- glm(pass ~ hours, data = df, family = "binomial")
predicted <- predict(reglogit, newdata = data.frame(hours), se.fit=TRUE)
linkinv <- family(reglogit)$linkinv
df$pred <- linkinv(predicted$fit)

g <- ggplot() +
  geom_line(data=df, aes(hours, pred)) +
  geom_point(data=df, aes(hours, pass)) +
  xlab("Horas de estudio") + ylab("Aprobado") +
  theme_bw()
print(g)

