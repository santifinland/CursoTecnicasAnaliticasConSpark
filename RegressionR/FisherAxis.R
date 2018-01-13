rng('default')
n = 1000;
# Simulation parameters
mu1 <- c(1, 3)
mu2 <- c(4, 1)
rho <- 0.3
s1 <- .8
s2 <- .5
Sigma <- matrix(c(s1^2, rho * s1 * s2, rho * s1 * s2, s2^2), byrow = TRUE, nrow = 2)
X1 = mvnrnd(mu1,Sigma,n);
X2 = mvnrnd(mu2,Sigma,n);
X = matrix(c(X1, X2), byrow = TRUE, nrow=2);
Y = matrix(c(zeros(n,1), ones(n,1)), brow=TRUE, nrow=2);
scatter(X1(:,1), X1(:,2), [], 'b' );
hold on
scatter(X2(:,1), X2(:,2), [], 'r' );
axis equal
m1 = mean(X(1:n,:))';
m2 = mean(X(n+1:end,:))';
plot(m1(1),m1(2),'bx','markersize',18)
plot(m2(1),m2(2),'rx','markersize',18)
plot([m1(1),m2(1)], [m1(2),m2(2)],'g')
%% classifier taking only means into account
w = m2 - m1; 
w = w / norm(w);
% project data onto w
X1_projected = X1 * w;
X2_projected = X2 * w;
% plot histogram and rotate it
angle = 180/pi * atan(w(2)/w(1));
[hy1, hx1] = hist(X1_projected);
[hy2, hx2] = hist(X2_projected);
hy1 = hy1 / sum(hy1); % normalize
hy2 = hy2 / sum(hy2); % normalize
scale = 4; % set manually
h1 = bar(hx1, scale*hy1,'b');
h2 = bar(hx2, scale*hy2,'r');
set([h1, h2],'ShowBaseLine','off')
% rotate around the origin
rotate(get(h1,'children'),[0,0,1], angle, [0,0,0])
rotate(get(h2,'children'),[0,0,1], angle, [0,0,0])