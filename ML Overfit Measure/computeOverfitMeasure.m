%David Kwon
%Computes overfit measure difference between dimensions 10 and 2. 
%Parameters: True Dimension of Target, Size of Train set, Size of Test set, Noise, Number of Experiments
function [ overfit_m ] = computeOverfitMeasure( true_Q_f, N_train, N_test, var, num_expts )

for i=1:num_expts
   [train_set test_set] = generate_dataset(true_Q_f,N_train,N_test,var);
   z10Train = computeLegPoly(train_set(:,1),10);
   a10 = glmfit(z10Train, train_set(:,2),'normal','constant','off');
   z10Test = computeLegPoly(test_set(:,1),10);
   E10 = sum((a10' * z10Test' - (test_set(:,2))').^2)/N_test; 
   
   z2Train = computeLegPoly(train_set(:,1),2);
   a2 = glmfit(z2Train, train_set(:,2),'normal','constant','off');
   z2Test = computeLegPoly(test_set(:,1),2);
   E2 = sum((a2' * z2Test' - (test_set(:,2))').^2)/N_test; 
   overfit_m(i) = E10 - E2; 
end