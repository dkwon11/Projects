%David Kwon 
function [ train_set test_set ] = generate_dataset( Q_f, N_train, N_test, sigma )

randomSet = -1 + 2*rand(N_train + N_test,1);

Z = computeLegPoly(randomSet,Q_f);

sumHolder = 0;
for i=0:Q_f
    sumHolder = sumHolder + 1/(2*i+1);
end
normalizer = sumHolder^(1/2);

A=normrnd(0,1,1,Q_f+1);

target = (A * Z'/normalizer)';

randEP = normrnd(0,1,N_train + N_test,1);

Y = target + sigma * randEP;

train_set = [randomSet(1:N_train),Y(1:N_train)];
test_set = [randomSet(N_train + 1: N_train+N_test),Y(N_train + 1: N_train+N_test)];