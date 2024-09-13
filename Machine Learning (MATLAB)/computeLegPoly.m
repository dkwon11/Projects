%David Kwon
function [ z ] = computeLegPoly( x, Q )

polyHolder(1:max(size(x)),1)= 1;
polyHolder(:,2)= x;

if Q >= 3
   for i= 2 : Q
       polyHolder(:,i+1)= (((2*i-1)/(i)) * x .* polyHolder(:,i)) - ((i-1)/i .* polyHolder(:,i-1));
   end
end

z = polyHolder;
