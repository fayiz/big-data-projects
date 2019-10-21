REGISTER decript_healthcare.jar;

A = LOAD 'pigdata/healthcare_Sample_dataset2.csv' using PigStorage(',') AS (PatientID: int, Name: chararray, DOB: chararray, PhoneNumber: chararray, EmailAddress: chararray, SSN: chararray, Gender: chararray, Disease: chararray, weight: float);
groupA = GROUP A all;
countA = FOREACH groupA generate count(A.PatientID);

B = LOAD 'pigdata/healthcare_Sample_dataset1.csv' using PigStorage(',') AS (PatientID: int, Name: chararray, DOB: chararray, PhoneNumber: chararray, EmailAddress: chararray, SSN: chararray, Gender: chararray, Disease: chararray, weight: float);
groupB = GROUP B all;
countB = FOREACH groupB generate count(B.PatientID);

C = UNION A, B;
groupC = GROUP C all;
countC = FOREACH groupC generate count(C.PatientID);

D = FOREACH C GENERATE PatientID, DeIdentifyUDF(Name,'12345678abcdefgh'), DeIdentifyUDF(DOB,'12345678abcdefgh'), DeIdentifyUDF(PhoneNumber,'12345678abcdefgh'), DeIdentifyUDF(EmailAddress,'12345678abcdefgh'),DeIdentifyUDF(SSN,'12345678abcdefgh'), DeIdentifyUDF(Disease,'12345678abcdefgh'),weight;
groupD = GROUP D all;
countD = FOREACH groupD generate count(D.PatientID);

E = FILTER D by PatientID == 11111;


STORE D into 'pigdata/deidentifiedDir';

