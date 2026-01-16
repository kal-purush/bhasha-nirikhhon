import java.util.*;
interface Account{
double interestrate=0.0;
double amount=0.0;
abstract double calculateInterest();
}
class SBAccount implements Account{
private double interestrate;
private double amount;
double gainedInterest;
public double calculateInterest(){
Scanner sc=new Scanner(System.in);
System.out.println("Enter the average in your account");
amount=sc.nextDouble();
System.out.println("Enter 1 for Normal account or 2 for NRI account");
int typeofacc=sc.nextInt();
if(amount <= 0)
{
System.out.println("Please enter Valid Amount.");
}
else{
if(typeofacc==1){
gainedInterest=((amount*4)/100);
System.out.println("Interest gained "+gainedInterest);
}
else if(typeofacc == 2){
gainedInterest=((amount*6)/100);
System.out.println("Interest gained "+gainedInterest);
}
else{
System.out.println("invalid selection");
}
}
return 0;
}
}

class FDAccount implements Account{
private double interestrate;
private double amount;
private int noOfDays;
int ageOfACHolder;
double gainedInterest;
public double calculateInterest(){
System.out.println("Enter the FD amount:");
Scanner sc=new Scanner(System.in);
amount=sc.nextDouble();
System.out.println("Enter the no of days:");
noOfDays=sc.nextInt();
System.out.println("Enter your age:");
ageOfACHolder=sc.nextInt();
if(amount<10000000){
if(noOfDays>=7 && noOfDays<=14){
if(ageOfACHolder<=60){
interestrate=4.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=5.0;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else if(noOfDays>=15 && noOfDays<=29){
if(ageOfACHolder<=60){
interestrate=4.75;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=5.25;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else if(noOfDays>=30 && noOfDays<=45){
if(ageOfACHolder<=60){
interestrate=5.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=6.00;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else if(noOfDays>=46 && noOfDays<=60){
if(ageOfACHolder<=60){
interestrate=7;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=7.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else if(noOfDays>=61 && noOfDays<=184){
if(ageOfACHolder<=60){
interestrate=7.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=8.00;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else if(noOfDays>=185 && noOfDays<=365){
if(ageOfACHolder<=60){
interestrate=8.00;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else{
interestrate=8.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
}
else {System.out.println("Please Provide Valid days.");}
}
else{
if(noOfDays>=7 && noOfDays<=14){
interestrate=6.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else if(noOfDays>=15 && noOfDays<=29){
interestrate=6.75;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else if(noOfDays>=30 && noOfDays<=45){
interestrate=6.75;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else if(noOfDays>=46 && noOfDays<=60){
interestrate=8.00;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else if(noOfDays>=61 && noOfDays<=184){
interestrate=8.50;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else if(noOfDays>=185 && noOfDays<=365){
interestrate=10.00;
gainedInterest=(amount*interestrate)/100;
System.out.println("Interest gained is: "+gainedInterest);
}
else {System.out.println("Please Provide Valid days.");}
}
return 0.0;}

}


class RDAccount implements Account{
private double interestRate;
private double amount;
private double monthlyAmount;
private int ageOfACHolder;
private int noOfMonths;

public double calculateInterest(){
Scanner sc = new Scanner(System.in);
System.out.println("Enter the RD amount: ");
amount = sc.nextDouble();
System.out.println("Enter the number of months, either 6 or 9 or 12 or 15 or 18 or 21");
noOfMonths = sc.nextInt();
System.out.println("Enter your age: ");
ageOfACHolder = sc.nextInt();
if(amount <= 0.0){
System.out.println("Invalid RD Amount");
}
else{
if (noOfMonths == 6) {
if(ageOfACHolder <= 55){
interestRate = 7.5;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
}
else if (noOfMonths == 9) {
if(ageOfACHolder <= 55){
interestRate = 7.75;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
interestRate = 8.25;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
}
else if (noOfMonths == 12) {
if(ageOfACHolder <= 55){
interestRate = 8.0;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
interestRate = 8.5;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
}
else if (noOfMonths == 15) {
if(ageOfACHolder <= 55){
interestRate = 8.25;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
interestRate = 8.75;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
}
else if (noOfMonths == 18) {
if(ageOfACHolder <= 55){
interestRate = 8.5;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
interestRate = 9.0;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}  
}
else if (noOfMonths == 21) {
if(ageOfACHolder <= 55){
interestRate = 8.75;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}
else{
interestRate = 9.25;
double gainedInterest = (amount * interestRate)/100;
System.out.println("Interest Gained is : "+gainedInterest);
}	  
}
else System.out.println("Please Provide Valid Months.");
}
return 0.0;		
}
}





public class InterestCalculator{
public static void main(String args[]){
try{
FDAccount fda=new FDAccount();
SBAccount sba=new SBAccount();
RDAccount rda=new RDAccount();
System.out.println("MAIN MENU \n 1. Interest Calculator - SB \n 2. Interest Calculator - FD \n 3. Interest Calculator - RD \n 4. Exit");
System.out.println("Enter your option(1..4): ");
Scanner sc=new Scanner(System.in);
int choice=sc.nextInt();

if(choice==1){
sba.calculateInterest();
}
else if(choice==2){
fda.calculateInterest();
}
else if(choice==3){
rda.calculateInterest();
}
else if(choice==4){
System.out.println("exiting");
System.exit(0);
}
else
System.out.println("Please enter valid choice");
}
catch(Exception e){
System.out.println("Error occurred at: "+e);
}
}
}