import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import datetime as dt


def create_immigration_calendar_dimension(df, output_data):
    """This function creates an immigration calendar based on arrival date

    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """
    # create udf to convert arrival date
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # create initial calendar df from arrdate column
    calendar_df = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()

    # expand df by adding other calendar columns
    calendar_df = calendar_df.withColumn('arrival_day', dayofmonth('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_week', weekofyear('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_month', month('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_year', year('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_weekday', dayofweek('arrdate'))

    # create an id field in calendar df
    calendar_df = calendar_df.withColumn('id', monotonically_increasing_id())

    # write the calendar dimension to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    calendar_df.dropDuplicates(['arrival_day']).write.parquet(output_data + "immigration_calendar", partitionBy=partition_columns, mode="overwrite")

    return calendar_df


def create_country_dimension_table(df, temp_df, output_data):
    """This function creates a country dimension from the immigration and global land temperatures data.

    :param df: spark dataframe of immigration events
    :temp_df: spark dataframe of global land temperatures data.
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """
    # get the aggregated temperature data
    new_temp_df = temp_df.select(['Country', 'AverageTemperature']).groupby('Country').avg()

    agg_temp = new_temp_df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature').toPandas()
    # load the i94res to country mapping data
    mapping_codes = pd.read_csv('i94res.csv')

    @udf('string')
    def get_country_average_temperature(name):
        print("Processing: ", name)
        avg_temp = agg_temp[agg_temp['Country']==name]['average_temperature']

        if not avg_temp.empty:
            return str(avg_temp.iloc[0])

        return None

    @udf()
    def get_country_name(code):
        name = mapping_codes[mapping_codes['code']==code]['Name'].iloc[0]

        if name:
            return name.title()
        return None

    # select and rename i94res column
    dim_df = df.select(['i94res']).distinct() \
        .withColumnRenamed('i94res', 'country_code')

    # create country_name column
    dim_df = dim_df.withColumn('country_name', get_country_name(dim_df.country_code))

    # create average_temperature column
    dim_df = dim_df.withColumn('average_temperature', get_country_average_temperature(dim_df.country_name))

    # write the dimension to a parquet file
    dim_df.write.parquet(output_data + "country", mode="overwrite")

    return dim_df

# Create visa dimension table with new_imm_df
def create_visa_type_dimension_table(df, output_data):
    """Function used for create a visatype dimension table from dataframe received, this case immigration
    :parameter: df: dataframe of immigration data
    :parameter: output_data: path to write dimension dataframe on it
    :return: spark dataframe to refer de calendar
    """
    dim_visatype_df = df.select(['visatype']).distinct() # create visatype df from visatype column
    dim_visatype_df = dim_visatype_df.withColumn('visa_type_key', monotonically_increasing_id()) # add an id column that increase 1 by 1
    dim_visatype_df.write.parquet(output_data + "visatype", mode="overwrite")# write to parquet file the dimension table
    return dim_visatype_df

# Defining function used to create a us demographics dimension table from the us cities demographics data on the dataset
def create_demogra_dim_table(df, output_data):
    """Purpose: to create an USdemographics dimension table from the US cities demographics provided
    :param df: dataframe of us demographics
    :param output_data: path to where write dimension dataframe
    :return:resulted dataframe to refer the dimension
    """
    dimension_df = df.withColumnRenamed('Median Age','median_age')
    dimension_df = dimension_df.withColumnRenamed('Male Population', 'male_population')
    dimension_df = dimension_df.withColumnRenamed('Female Population', 'female_population')
    dimension_df = dimension_df.withColumnRenamed('Total Population', 'total_population')
    dimension_df = dimension_df.withColumnRenamed('Number of Veterans', 'number_of_veterans')
    dimension_df = dimension_df.withColumnRenamed('Foreign-born', 'foreign_born')
    dimension_df = dimension_df.withColumnRenamed('Average Household Size', 'average_household_size')
    dimension_df = dimension_df.withColumnRenamed('State Code', 'state_code')
    dimension_df = dimension_df.withColumn('id', monotonically_increasing_id()) # lets add an id column that increase 1 by 1
    dimension_df.write.parquet(output_data + "demographics", mode="overwrite") # write to parquet file the dimension table
    return dimension_df

# Function that will be used on the privious section
def get_visa_type_dimension(spark, output_data):
    return spark.read.parquet(output_data + "visatype")

def create_immi_fact_table(spark, df, output_data):
    """Functionto be used on creating an country dimension table from/for the immigration and global temperatures.
    :param df: spark dataframe of immigration
    :param visa_type_df:dataframe of global temperature.
    :param output_data: path to where write dimension dataframe
    :return:dataframe
    """
    dimension_df = get_visa_type_dimension(spark, output_data).toPandas() # get visa_type dimension from the dataframe
    @udf('string')
    #function that will be used in the following
    def getting_visa_key(visa_type):
        """Function to get visa key defined
        :param visa_type: dataframe US non-immigrant visa type declared
        :return: related visa key
        """
        key = dimension_df[dimension_df['visatype']==visa_type]['visa_type_key']
        if not key.empty:
            return str(key.iloc[0])
        return None

    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)# converting arrival date in SAS format to datetime object with udf
    df = df.withColumnRenamed('cicid','record_id') # rename columns to align with data model
    df = df.withColumnRenamed('i94res', 'country_residence_code') # rename columns to align with data model
    df = df.withColumnRenamed('i94addr', 'state_code') # rename columns to align with data model
    df = df.withColumn('visa_type_key', getting_visa_key('visatype')) # create visa_type key
    df = df.withColumn("arrdate", get_datetime(df.arrdate))# convert arrival date into datetime object
    df.write.parquet(output_data + "immigration_fact", mode="overwrite")# write to parquet file dimension
    return df

def create_corruption_dim_table(df, output_data):
    """Purpose: to create an world corruption dimension table
    :param df: dataframe of us demographics
    :param output_data: path to where write dimension dataframe
    :return:resulted dataframe to refer the dimension
    """
    map_codes = pd.read_csv('i94res.csv')

    # Title name
    map_codes['Name'] = map_codes['Name'].str.title()

    @udf()
    def get_code(name):
        code = map_codes[map_codes['Name'] == name]['code']
        if not code.empty:
            return str(code.iloc[0])
        else:
            return None

    # create country_id column
    cpi_df = df.withColumn('country_id', get_code(df.Country))

    # write dimension to parquet file
    cpi_df.dropDuplicates(['country_id']).write.parquet(output_data + "corruption", mode="overwrite")
    return cpi_df


# Perform quality checks here
def quality_checks(spark, table_name, table_id, output_data):
    """Count checks on fact and dimension table to ensure completeness of data.
    :param df: spark dataframe to check counts on
    :param table_name: corresponding name of table
    """
    df = spark.read.parquet(output_data + table_name)
    total_count = df.count()
    total_count_droped = df.dropDuplicates([table_id]).count()
    
    if total_count == 0:
        print(f"Data quality check failed for {table_name} with zero records!")
    elif total_count != total_count_droped:
        print(f"Integrity constraints check failed for {table_name} with {total_count - total_count_droped} duplicate records!")
    else:
        print(f"Data quality check passed for {table_name} with {total_count:,} records.")
    
