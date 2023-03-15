#  <span style="color:tan"> **Module 22 Big Data ETL Challenge**  </span>
### Chris Gruenhagen 6Mar2023
---

## **Purpose**
The purpose of this assignment is to work with big data by extracting Amazon customer review datasets, transforming each dataset into four Data Frames, and loading the DataFrames into an Relational Database Service (RDS) instance. As a bonus, the selected data can be analyzed using SQL or PySpark to evaluate whether the reviews from Amazon's Vine program are trustworthy and free of bias.

## **Process**

### **Extract the Data**

    Amazon customer review datasets for Pet Products and Outdoor were extracted from the AWS site into Spark DataFrames in Google Colab.  Pet Products contained 2.6 million reviews and Outdoors contained 2.3 million reviews.  

### **Transform and Load the Data**

    Four DataFrames were created from the original DataFrame and uploaded into an AWS database.
| DataFrame | Desription| Columns | 
|------|------|------|
| review_id| primary table for customer product reviews | review_id, customer_id, product_id, product_parent, review_date |
| products| product names | product_id, product_title |
| customers | the number of reviews per customer| customer_id, customer_count |
| vine | individual review star rating, helpful and total votes, whether the reviewer is a Vine participant | review_id, star_rating, helpful_votes, total_votes, vine |


### **Analysis**

    For a first look, the review data was grouped into regular reviewers and vine reviewers.  The average star rating, helpful votes, total votes were calculated for each group.  The ratio of helpful votes to total votes was calculated as an estimate of review trustworthiness. The summary data was compared between the regular and vine reviewers to see if there were extreme differences between the two groups.

    Additional analyses
      * Evaluate individual reviewers: join vine and reviewer_id table and group by individual reviewer to check individual reviewer helpful_vote_ratios
     * Evaluate review content: update the database with the review content (review_body) to check for patterns of fraud, i.e. duplicated reviews, nonsense reviews, etc.
 
## **Results** 

Evaluate Pet Product reviews between vine users and non-vine users

All reviews 

|vine|avg_star_rating|avg_total_votes|avg_helpful_votes|helpful_vote_ratio|
|------|------|------|------|------|
|   Y|           4.07|           2.34|             1.96|             83.77|
|   N|           4.14|           1.94|             1.65|             85.26| 

    The regular and vine reviewers average star ratings are similar, indicating that the vine reviewer ratings are not inflated despite receiving the products for free.
    The regular and vine reviewers helpful vote ratios (helpful votes/total votes) are similar, indicating that the vine reviewer ratings not viewed as more or less helpful than the regular reviewers.  In this case, helpfulness votes are a proxy for truthfulness of the review.

All reviews with votes 

|vine|n_users|avg_star_rating|avg_total_votes|avg_helpful_votes|helpful_vote_ratio|
|------|------|------|------|------|------|
|   Y|   5026|           3.95|           4.76|             3.99|             83.77|
|   N| 974146|           3.83|           5.23|             4.46|             85.26|

    Regular and vine reviews ratings and votes are similar to each other in this data subset. Note that the number of vine reviews represent only 1% of this data.

Reviews with >100 votes

|vine|n_users|avg_star_rating|avg_total_votes|avg_helpful_votes|helpful_vote_ratio|
|------|------|------|------|------|------|
|   Y|     27|           3.93|          216.3|            206.7|             95.57|
|   N|   4561|            3.9|          236.7|           223.55|             94.44| 

    Regular and vine reviews ratings and votes are similar to each other in this data subset. Note that the number of vine reviews represent only 1% of this data.

Reviews with >1000 votes

|vine|n_users|avg_star_rating|avg_total_votes|avg_helpful_votes|helpful_vote_ratio|
|------|------|------|------|------|------|
|   Y|      1|            4.0|         1277.0|           1239.0|             97.02|
|   N|     77|           3.96|        1619.64|          1562.19|             96.45| 

    Regular and vine reviews ratings and votes are similar to each other in this data subset. Note that the number of vine reviews represent only 1% of this data.

---

## **Summary** 

 A preliminary analysis of the Pet Products reviews from Vine reviewers and regular reviewers indicates that the average rating and trustworthiness (as measured by proxy) are similar between the two groups.  The Vine reviews do not appear to show bias as compared to the regular reviews.
 
 Additional analyses could be performed to evaluate trustworthiness of individual reviewers and/or evaluate review content for anomalies.  

---

# **Continue the Data Preparation Cheat Sheet**
Data Preparation Cheat Sheet
1. check datatypes (numeric cols may have non-numeric info that needs to be removed, data types may need to be changed) 
    df.dtypes

2. replace categorical with numeric values - consider replacing empty strings with NaN 
    https://www.geeksforgeeks.org/how-to-convert-categorical-variable-to-numeric-in-pandas/


    df['colname'].unique()
    df['colname'].replace(['uniquevalue1', 'uniquevalue2','uniquevalue3',''],
                        [1,2,3,0], inplace=True)

    OR
    
    dummies = pd.get_dummies(df.colname)
    merged = pd.concat([df,dummies].axis='columns')
    merged.drop('colname',axis='columns')

    OR (example from homework Mod20-Day1-Activity02)

    df['colname'].unique()
    colname_dict = {'uniquevalue1': 1, 'uniquevalue2': 2, 'uniquevalue3': 3}
    df2 = df.replace({'colname': colname_dict})
     note: not sure how this would handle a null

    OR (example from homework Mod20-Day1-Activity03)

    def changeChannel(channel):
        if channel == "uniquevalue1":
            return 1
        if channel == "uniquevalue2":
            return 2
        if channel == "uniquevalue3":
            return 2        
        else:
            return 0 
    df_shopping["Channel"] = df_shopping["Channel"].apply(changeChannel)

3. check for nas, nulls and remove if necessary  (na = NOT A NUMBER)  NOTE: df.isna() == df.isnull()  
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html

    df.isna().sum()
    df.dropna() 

    NOTE: This does not detect empty strings.  To detect empty strings search for df['colname']==''
    https://stackoverflow.com/questions/27159189/find-empty-or-nan-entry-in-pandas-dataframe

4. check for duplicate values and remove if necessary
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html

    df.duplicated().sum()
    df.drop_duplicates(keep='first')  note: keep = 'first' is default


5. drop unnecessary columns 
    https://www.w3schools.com/python/pandas/ref_df_drop.asp#:~:text=The%20drop()%20method%20removes

    df.drop(columns=['colname'])
    or
    df.drop('colname',axis = 1)
    df.drop('colname',axis = 'columns')

6. standardize dataset so larger numbers don't influence the outcome more
    Scale the data - all columns (example from homework Mod20-Day1-Activity06)
        df_scaled = StandardScaler().fit_transform(df)

    Scale the data - subset of columns (example from homework Mod20-Day1-Activity03)
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(df_shopping[['Fresh', 'Milk', 'Grocery', 'Frozen', 'Detergents_Paper', 'Delicatessen']])

        If you need to add back a column that wasn't part of the scaling (was already 0,1)

        # A list of the columns from the original DataFrame
        df_shopping.columns

        # Create a DataFrame with the transformed data
        new_df_shopping = pd.DataFrame(scaled_data, columns=df_shopping.columns[1:])
        new_df_shopping['Channel'] = df_shopping['Channel']
        new_df_shopping.head()

---

#  &#x1f52e; &#x1f520; &#x1f523; 🔢 📈 &#x1F469;&#x200d;&#x1F4bb; 📉 🔢 &#x1f523; &#x1f520; &#x1f52e;
     emoji & format references:
        https://emojipedia.org
        https://commons.wikimedia.org/wiki/Emoji/Table
        https://www.markdownguide.org/basic-syntax/

        
***See below for original homework instructions***
# <span style="color:tan"> Module 22 Big Data ETL Challenge</span>
Due March 15, 2023 by 11:59pm 

Points 100 

Submitting a text entry box or a website url
_________________________________________________________
## <span style="color:red"> **Background**  </span>

In this assignment, you will put your ETL skills to the test. Many of Amazon's shoppers depend on product reviews to make a purchase. Amazon makes these datasets publicly available. They are quite large and can exceed the capacity of local machines. One dataset alone contains over 1.5 million rows; with over 40 datasets, data analysis can be very demanding on the average local computer. Your first goal will be to perform the ETL process completely in the cloud and upload a DataFrame to an RDS instance. The second goal will be to use PySpark or SQL to perform a statistical analysis of selected data.

This Challenge contains two parts. Part 1 is required. Part 2 is optional but highly recommended to strengthen your new skills.

* Part 1: Extract two Amazon customer review datasets, transform each dataset into four DataFrames, and load the DataFrames into an RDS instance.

* Part 2: Extract two Amazon customer review datasets and use either SQL or PySpark to analyze whether reviews from Amazon's Vine program are trustworthy.

##  <span style="color:orange"> **Before You Begin** </span>
1. Create a new repository for this project called "Big-Data-ETL". Do not add this homework to an existing repository.

2. Clone the new repository to your computer.

3. Inside your local Git repository, create two folders, "part-1" and "part-2", corresponding to the two parts. If you are not planning on doing "part-2" then create the "part-1" folder only.

##  <span style="color:yellow"> **Files** </span>

Download the following files to help you get started:

<a href = "https://static.bc-edx.com/data/dl-1-1/m22/lms/starter/Starter_Code_v1.zip " target = "_blank"> Module 22 Challenge files </a>


##  <span style="color:green">  **Instructions** </span>
###  <span style="color:green">  **Part 1** </span>
1. Upload the part_one_starter_code.ipynb into Google Colab and create a duplicate of this file.

2. Explore the <a href = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt" target = "_blank"> Amazon Reviews </a> datasets and pick two datasets to perform ETL.

3. Rename each part_one_starter_code.ipynb file according to the dataset you are using. For example, if you are going to use the <a href = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_Games_v1_00.tsv.gz" target = "_blank"> Video Game Reviews </a>  file, rename file, part_one_video_games.ipynb. Repeat the process for the duplicate file you created in Step 2.


### ***Extract the Data***
1. Read in each dataset using the correct header and sep parameters.

2. Get the number of rows in the dataset.

### ***Transform the Data***
For each dataset use the schema.sql file located in the Resources folder of the Starter_Code.zip file to create the four DataFrames as follows:

1. Create the "review_id_df" DataFrame with the appropriate columns and data types.

2. Create the "products_df" DataFrame that drops the duplicates in the "product_id" and "product_title columns.

3. Create the "customers_df" DataFrame that groups the data on the "customer_id" by the number of times a customer reviewed a product.

4. Create the "vine_df" DataFrame that has the "review_id", "star_rating", "helpful_votes", "total_votes", and "vine" columns.

### ***Load the Data into an RDS Instance***

Export each DataFrame into the RDS instance to create four tables for each dataset.

**Note:** This process can take up to 10 minutes for each. Ensure that everything is correct before uploading.

###  <span style="color:green">  **Part 2** </span>

Recall that this part is completely optional; you can complete it as a way to challenge yourself and boost your new skills.

In Amazon's Vine program, reviewers receive free products in exchange for reviews. Amazon has several policies to reduce the bias of its Vine reviewsLinks to an external site..

<img src = 'https://static.bc-edx.com/data/dl-1-1/m22/lms/img/vine01.jpg' alt = 'image of vine post' />

But are Vine reviews truly trustworthy? Your task is to investigate whether Vine reviews are free of bias. Use either PySpark or, for an extra challenge, SQL to analyze the data.

* If you choose SQL, first use Spark on Colab to extract and transform the data and then load it into a SQL table on your RDS account. Perform your analysis with SQL queries on RDS.

* While there are no strict requirements for the analysis, consider steps you can take to reduce noisy data, such as filtering for reviews that meet a certain number of helpful votes, total votes, or both.

* Submit a summary of your findings and analysis.


##  <span style="color:blue"> **Requirements** </span>
These requirements are for Part 1 only, as Part 2 is optional.

### Extract (35 points)
* Uses PySpark to connect to and load the AWS datasets into DataFrames. (10 points)
* The correct parameters are used to read in the data into a DataFrame. (15 points)
* The first 20 rows of each DataFrame is displayed. (5 points)
* The number of rows for each DataFrame is displayed. (5 points)
### Transform (45 points)
* The "review_id_df" DataFrame is created with the appropriate columns and data types. (15 points)
* The "products_df" DataFrame is created and the the duplicate values are dropped. (10 points)
* The "customers_df" DataFrame is created and displays the number of times a customer reviewed a product grouped by the "customer_id". (10 points)
* The "vine_df" DataFrame is created that has the "review_id", "star_rating", "helpful_votes", "total_votes", and "vine" columns. (10 points)
### Load (20 points)
* The four DataFrames for each dataset are successfully loaded into an RDS instance. (20 points)

## <span style="color:indigo"> **Submission**  </span>
* Download your Google Colab notebooks as .ipynb files and upload them into the "part-1" folder of your "Big-Data-ETL" Git repository.

IMPORTANT

    Do not clear the outputs of your .ipynb files, and do not upload notebooks that contain your RDS password and endpoint. Delete these two items before making your notebook public!

* Ensure your repository has regular commits and a thorough README.md file to explain the ETL project.

* If you are doing "part-2" of this assignment, copy your SQL queries into .sql files and upload them into the "part-2" folder of your "Big-Data-ETL" Git repository.

IMPORTANT

    Remember to closely monitor any AWS resources that you choose to use! It’s crucial that you clean up and stop, or shut down any AWS resources to avoid accruing additional costs. Please refer to the AWS_cleanup.pdf and the AWS_check_billing.pdf files in the Resources folder of the Starter_Code.zip file. Or, you can download the
<a href = https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.1/AWS_check_billing.pdf target = _blank > AWS Billing Guide </a>


To submit your Challenge assignment, click Submit, and then provide the URL of your GitHub repository for grading.


## <span style="color:violet"> **References**  </span>
Amazon Customer Reviews Dataset. (n.d.). Retrieved April 08, 2021, from: <a href = 'https://s3.amazonaws.com/amazon-reviews-pds/readme.html' target = '_blank'>  https://s3.amazonaws.com/amazon-reviews-pds/readme.html  </a>

© 2023 edX Boot Camps LLC

