# US-CLV-Margin-Model-2.0
This was created for US CLV Margin Model 2.0

Objective: Predict future vehicle Contribution Margin for US Ford customers. 
	Enhancements to the existing CLV model.
Region: Ford US
Resources: Pelham, Chelsea, Bharath, Partha, Gary

Model Methodology: 
Data sources: VOMART
Tools used: PySpark | DataRobot
Model development: Using ML methods, developed model to estimate profit amount at a customer level

Deliverables | Implementation:
The model will be used to score the entire customer base for Ford USA and will be integrated with other models being developed in the CLV space for a comprehensive CLV 2.0

Steps:
1. The data preparation for the US NA CLV Margin Model is located in the file named - US CLV Margin Model OOT - Governance code
2. The Training Output is taken to DatRobot as a CSV file and teh model is developed in DatRobot.
3. The Test Output is taken to DatRobot for testing the model.
4. The model is scored for the latest data for customers 2022 and beyond and compared on a similar time period with the "current model" for comapring the sMAPEs.

The files for the sMAPE comparison of the new mode for customers 2022 & beyond as well as the current model file is also attached.

The Scoring code for Canadian customers is also attached
