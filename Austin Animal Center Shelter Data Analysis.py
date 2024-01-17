# CLEAN DATA

# Import pandas and numpy library
import pandas as pd
import numpy as np
import datetime

# Read file aac_intakes.csv and save as "aac_intakes"
aac_intakes = pd.read_csv("D:\My Documents\Downloads\Day_5\Demo/aac_intakes.csv")
# print(aac_intakes)

# Read file aac_outcomes.csv and save as "aac_outcomes"
aac_outcomes = pd.read_csv("D:\My Documents\Downloads\Day_5\Demo/aac_outcomes.csv")
# print(aac_outcomes)

# Update the lastest time each animal appear in dataset and save them to another dataframe
    # Convert to appropriate datatypes
aac_intakes["datetime"] = pd.to_datetime(aac_intakes["datetime"])
aac_intakes["datetime2"] = pd.to_datetime(aac_intakes["datetime2"])
aac_outcomes["datetime"] = pd.to_datetime(aac_outcomes["datetime"])
aac_outcomes["date_of_birth"] = pd.to_datetime(aac_outcomes["date_of_birth"])
aac_outcomes["monthyear"] = pd.to_datetime(aac_outcomes["monthyear"])
    # print (aac_intakes.info())
    # print (aac_outcomes.info())

    # Drop duplicates row and NaN rows
aac_intakes = aac_intakes.drop_duplicates()
aac_intakes = aac_intakes.dropna(subset = ["animal_id","datetime","datetime2"])

aac_outcomes = aac_outcomes.drop_duplicates()
aac_outcomes = aac_outcomes.drop_duplicates(subset = ["animal_id","datetime","date_of_birth", "monthyear"])
# print(aac_intakes.info())
# print(aac_outcomes.info())

    # Record the latest time each animal appear in the dataset
id_intakes = aac_intakes.animal_id
duprows_aac_intakes = aac_intakes[id_intakes.isin(id_intakes[id_intakes.duplicated()])].sort_values("animal_id")
last_aac_intakes = duprows_aac_intakes.groupby("animal_id")["datetime"].idxmax()
new_last_aac_intakes = duprows_aac_intakes.loc[last_aac_intakes]
nonDup_aac_intakes = aac_intakes.drop_duplicates(subset = ["animal_id"],keep = False)

id_outcomes = aac_outcomes.animal_id
duprows_aac_outcomes = aac_outcomes[id_outcomes.isin(id_outcomes[id_outcomes.duplicated()])].sort_values("animal_id")
last_aac_outcomes = duprows_aac_outcomes.groupby("animal_id")["datetime"].idxmax()
new_last_aac_outcomes = duprows_aac_outcomes.loc[last_aac_outcomes]
nonDup_aac_outcomes = aac_outcomes.drop_duplicates(subset =["animal_id"],keep = False)

    # Save to a new dataframe
final_aac_intakes = pd.concat([new_last_aac_intakes,nonDup_aac_intakes])
# print (final_aac_intakes)
final_aac_outcomes = pd.concat([new_last_aac_outcomes, nonDup_aac_outcomes])
# print (final_aac_outcomes)


# ANALYSE DATA
#### Question number 1: What is the distribution of the types of animals in the shelter?
animal_type_distribution = final_aac_intakes.groupby(by = "animal_type").count().animal_id
print("Q1. Distribution of animal by type:")
print(animal_type_distribution)

#### Question number 2: Is there an area where more pets are found?. 
#### Find the top 5 location where pet are found
found_location_distribution = final_aac_intakes.groupby(by = "found_location").count().animal_id.sort_values(ascending = False).head(5)
print("Q2. Distribution of animal by location:")
print(found_location_distribution)

#### Question number 3: What is the average number of pets found in a month in the year 2015?. 
#### Are there months where there is a higher number of animals found?
data2015 = final_aac_intakes[final_aac_intakes.datetime.dt.year == 2015]
data_months = data2015.groupby(by = data2015.datetime.dt.month_name()).count().animal_id.sort_values()
average_month = int(data2015.count().animal_id/12)
print("Q3. Distribution of animal by months:")
print("Number of animals by month: \n", data_months)
print("Average number of animals by month: ", average_month)

#### Question number 4: What is the ratio of incoming pets vs. adopted pets
adoption = final_aac_outcomes[final_aac_outcomes.outcome_type == "Adoption"].count().animal_id
ratio = round(adoption/final_aac_outcomes.count().animal_id*100,2)
print ("Q4. Ratio of adoption pets versus incoming pets: ", ratio, "%")


#### Question number 5: What are the adoption rates for specific breeds?
#### Find the top 5 dog breeds in the shelter (based on count) 
#### Find the adoption percentage of each breed.
    # Filter data with dogs type
data_dogs = final_aac_outcomes[final_aac_outcomes.animal_type == "Dog"]
    # Identify all types of breeds
distribution_by_breeds = data_dogs.breed.unique()
    # Declare list and dataframe
breed_list = pd.DataFrame()
breed =[]
adoption = []
    # Count adoption and total number for each breed
for each_breed in distribution_by_breeds:
    data_breed = data_dogs[data_dogs.breed == each_breed]
    count_adoption = data_breed[data_breed.outcome_type == "Adoption"].count().animal_id
    count = data_breed.count().animal_id
    breed.append(count)
    adoption.append(count_adoption)
    # Create a table to store all data
breed_list = breed_list.assign (
    Breed_type = distribution_by_breeds,
    Total_number = breed,
    Total_adoption = adoption,
)
    # Calculate adoption rate and sort top 5 dog breeds
breed_list["Adoption_percentage"] = breed_list["Total_adoption"]/breed_list["Total_number"]*100
breed_list = breed_list.sort_values(by = ["Total_number"], ascending = False).head(5)
breed_list = breed_list.reset_index(drop = True)
print("Q5. Top 5 dog breeds and adoption rate: ")
print(breed_list)

#### Question number 6: About how many animals are spayed/neutered each month?
#### This will help the shelter allocate resources and staff. 
#### Assume that all intact males and females will be spayed/neutered.
    # Slice small data to see the 'sex_upon_intake' and 'sex_upon_outcome' column
filter_intake = aac_intakes[['animal_id', 'sex_upon_intake']]
filter_outcome = aac_outcomes[['animal_id', 'sex_upon_outcome','datetime']]
    # Drop duplicate values in the first time that animal's information had record
new_filter_intake = filter_intake.drop_duplicates(subset = ['animal_id'])
    # Select the last date that animal's information had record
new_filter_outcome = filter_outcome.sort_values(by = 'datetime')
new_filter_outcome = filter_outcome.drop_duplicates(subset = ['animal_id'], keep = 'last').sort_values (by = "datetime")
    # Intersection 2 dataframe to see the each column values
combined_data = pd.merge(new_filter_intake, new_filter_outcome, how ='inner', on =['animal_id'])
    # Find the animal have not spayed/neutered before
intake_no_sp_ne = combined_data [(combined_data.sex_upon_intake != 'Spayed Female') | (combined_data.sex_upon_intake != 'Neutered Male')]
    # Find the animal have spayed/neutered after leave in shelter
outcome_sp_ne = intake_no_sp_ne[(intake_no_sp_ne.sex_upon_outcome == 'Neutered Male') | (intake_no_sp_ne.sex_upon_outcome == 'Spayed Female')]
    # Find the animal have spayed/neutered by staff in shelter
final_filter = outcome_sp_ne[((outcome_sp_ne.sex_upon_intake != 'Neutered Male') & (outcome_sp_ne.sex_upon_outcome == 'Neutered Male')) 
                | ((outcome_sp_ne.sex_upon_intake != 'Spayed Female') & (outcome_sp_ne.sex_upon_outcome == 'Spayed Female'))]
    # Find the first/last time record in data 
first_time_record = final_filter['datetime'].min()
last_time_record = final_filter['datetime'].max()

    # Convert gap between two records into month
month_number = round((last_time_record - first_time_record)/(np.timedelta64(1, 'D')*30),0)
# print (month_number)
# print (final_filter.shape[0])
    # Display the result
print('Q6. There are',round(final_filter.shape[0] / month_number,2), 'pets had neutered/spayed each month')

#### Question number 7: How many animals in the shelter are repeats?, Which animal was returned to the shelter the most? 
#### This means the animal has been brought in more than once.
    # Remove animal with one record, the rest are animal repeats
animal_repeat_count = pd.DataFrame(aac_intakes.groupby(by = "animal_id").count().animal_type)
animal_repeat_count = animal_repeat_count[animal_repeat_count.animal_type != 1]
print("Q7. Number of animal repeats: ", animal_repeat_count.shape[0], "- corresponding to ", animal_repeat_count.animal_type.sum(), " repeats" )
    # Get the animal type with highest repeats
animal_type_count = pd.DataFrame(aac_intakes.groupby(by = "animal_type").count().animal_id)
print(animal_type_count['animal_id'].idxmax(), "is the most animal to be returned to the shelter")

#### Question number 8: What are the adoption rates for different colorings?
#### Find the top 5 colorings in the shelter (based on count) and then find the adoption percentage of each color

adoption_color_table = pd.DataFrame()
color_list = []
total_color_list = []
adoption_color_list = []
color_unique = final_aac_outcomes.color.unique()
for each in color_unique:
    color_list.append(each)
    final_list = final_aac_outcomes[final_aac_outcomes.color == each]
    total_color = final_list.count().animal_id
    total_color_list.append (total_color)
    adoption_color = final_list[final_list.outcome_type == "Adoption"].count().animal_id
    adoption_color_list.append (adoption_color)
adoption_color_table = adoption_color_table.assign (
    Color = color_list,
    Total_number = total_color_list,
    Adoption_number = adoption_color_list)
adoption_color_table ["Adoption_rate"] = round (adoption_color_table["Adoption_number"]/adoption_color_table["Total_number"],2)
adoption_color_table = adoption_color_table.drop(adoption_color_table[adoption_color_table.Adoption_number == 0].index)
adoption_color_table = adoption_color_table.sort_values(by = 'Total_number', ascending = False).head(5)
print ("Q8. Adoption rates by colorings: ", adoption_color_table.reset_index(drop=True))


#### Question number 9: What are the adoption rates for the following age groups?
# - baby: 4 months and less
# - young: 5 months - 2 years
# - adult: 3 years - 10 years
# - senior: 11+

    # Find the animal had adoption in outcome_type
adoption_aac_outcomes =  aac_outcomes[aac_outcomes.outcome_type == 'Adoption']
    # Drop NA values
adoption_aac_outcomes = adoption_aac_outcomes[adoption_aac_outcomes['age_upon_outcome'].notna()]
    # Find the data contain 'month' charater
age_upon_months = adoption_aac_outcomes[adoption_aac_outcomes['age_upon_outcome'].str.contains('month')]
    # Find the data contain 'year' charater
age_upon_years = adoption_aac_outcomes[adoption_aac_outcomes['age_upon_outcome'].str.contains("year")]
    # Combine 2 data frame above, we have animal data upper 1 month
Upper_1_month = pd.concat([age_upon_months, age_upon_years])
    # Subtract rows in 2 datafames to get animal data below 1 month
Lower_1_month = pd.concat([adoption_aac_outcomes,Upper_1_month]).drop_duplicates(keep=False)
    # Filter the data between baby, young, aldult and senior animal by index number.
list_Baby_index = []
list_Young_index = []
list_Aldult_index = []
list_Senior_index = []
    # Find data in age_upon_months datafame
        # Between 1 month and 1 year
for i in range(age_upon_months.shape[0]):
    List_separate_values = age_upon_months.age_upon_outcome.iloc[i].split(' ')
    if int(List_separate_values[0]) >= 5:
        list_Young_index.append(age_upon_months.age_upon_outcome.index[i])
    else:
        list_Baby_index.append(age_upon_months.age_upon_outcome.index[i]) 
        # Upper 1 year
for i in range(age_upon_years.shape[0]):
    List_separate_values = age_upon_years.age_upon_outcome.iloc[i].split(' ')
    if int(List_separate_values[0]) < 3:
        list_Young_index.append(age_upon_years.age_upon_outcome.index[i])
    elif 3 <= int(List_separate_values[0]) <= 10:
        list_Aldult_index.append(age_upon_years.age_upon_outcome.index[i])
    else: 
        list_Senior_index.append(age_upon_years.age_upon_outcome.index[i])
    # Display result
print ("Q9. Adoption rates by group of growth: ")    
print('Baby rate: ',round(100 / adoption_aac_outcomes.shape[0] * int(len(list_Baby_index) + Lower_1_month.shape[0]),3), '%')
print('Young rate: ',round(100 / adoption_aac_outcomes.shape[0] * int(len(list_Young_index)),3), '%')
print('Aldult rate: ',round(100 / adoption_aac_outcomes.shape[0] * int(len(list_Aldult_index)),3), '%')
print('Senior rate: ',round(100 / adoption_aac_outcomes.shape[0] * int(len(list_Senior_index)),3), '%')


#### Question number 10: If spay/neuter for a dog costs $100 and a spay/neuter for a cat costs $50,
#### How much did the shelter spend in 2015 on these procedures?'''

    # Get columns 'animal_id', 'sex_upon_intake' in Update_aac_intakes and 'animal_id', 'sex_upon_outcome','datetime' in Update_aac_outcomes
short_Update_aac_intakes = final_aac_intakes[['animal_id', 'sex_upon_intake']]
short_Update_aac_outcomes = final_aac_outcomes[['animal_id', 'sex_upon_outcome','datetime', 'animal_type']]
    # Intersection tow short data above
intersection_data = pd.merge(short_Update_aac_intakes, short_Update_aac_outcomes, how ='inner', on =['animal_id'])
    # Find the animal have not spayed/neutered before
intake_no_sp_ne = intersection_data.loc[(intersection_data.sex_upon_intake != 'Spayed Female') & (intersection_data.sex_upon_intake != 'Neutered Male')]
    # Find the animal have spayed/neutered after leave in shelter
outcome_sp_ne = intake_no_sp_ne[intake_no_sp_ne['sex_upon_outcome'].isin(['Neutered Male','Spayed Female'])]
    # Select data from year = 2015
filter_data_2015 = outcome_sp_ne[(outcome_sp_ne['datetime'] > "2015-01-01") & (outcome_sp_ne['datetime'] < "2015-12-31")]
    # Group data by 'animal_type'
number_SpNe = pd.DataFrame(filter_data_2015.groupby(filter_data_2015.animal_type).count())
    # Caculated the cost 
list_Cost = []
countNumber = 0
for value in number_SpNe.index.tolist():
    if number_SpNe.index[countNumber] == 'Dog':
        list_Cost.append(int(number_SpNe.iloc[countNumber].animal_id) * 100)
    if number_SpNe.index[countNumber] == 'Cat': 
        list_Cost.append(int(number_SpNe.iloc[countNumber].animal_id) * 50)
    countNumber = countNumber + 1
    # Display result
print('Q10. Total cost for spayed/neutered in 2015 is: ',sum(list_Cost), '$')