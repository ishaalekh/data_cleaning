# Databricks notebook source
File_path = '/FileStore/tables/LoanStats_2018Q4.csv'

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(File_path)

# COMMAND ----------

df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

temp_table_name = 'LoanStats'
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df_Sel = df.select('term','home_ownership','grade','purpose','int_rate','addr_state','loan_status','application_type','loan_amnt','emp_length','annual_inc','dti','dti_joint','delinq_2yrs','revol_util','total_acc','num_tl_90g_dpd_24m')

# COMMAND ----------

df_Sel.describe().show()

# COMMAND ----------

df_Sel.describe('term','loan_amnt','emp_length','annual_inc','dti','delinq_2yrs','revol_util','total_acc').show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct emp_length from LoanStats;

# COMMAND ----------

# MAGIC %md
# MAGIC So basically in the previous example, we are getting lenght as a string, we need to convert it into a number to make anakysis easy

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace, col

regex_string = 'years|year|\\+|\\<'
df_Sel.select(regexp_replace(col('emp_length'),regex_string,'')).alias('emp_length_cleaned').show()

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace, col
regex_string = '\\d+'
df_Sel.select(regexp_extract(col('emp_length'),regex_string,0)).alias('emp_length_cleaned').show()


# COMMAND ----------

df_Sel = df_Sel.withColumn('term_cleaned', regexp_replace(col('term'),'months','')).withColumn('emp_length_cleaned' ,regexp_extract(col('emp_length'),regex_string,0))

# COMMAND ----------

df_Sel.display()

# COMMAND ----------

table_name = 'loanstatus_Sel'
df_Sel.createOrReplaceTempView(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstatus_Sel;

# COMMAND ----------

df_Sel.stat.cov('annual_inc','loan_amnt')

# COMMAND ----------

# MAGIC %md
# MAGIC the output of the previous code is very high and doesnt give much insight about their relation, due to which its better to use correlation

# COMMAND ----------

df_Sel.stat.corr('annual_inc','loan_amnt')

# COMMAND ----------

# MAGIC %md
# MAGIC We can also use correlation in sql 

# COMMAND ----------

# MAGIC %sql
# MAGIC select corr(loan_amnt,term_cleaned) from loanstatus_Sel;

# COMMAND ----------

df_Sel.stat.crosstab('loan_status','grade').show()

# COMMAND ----------

freq = df_Sel.stat.freqItems(['purpose','grade'],0.3)


# COMMAND ----------

freq.collect()

# COMMAND ----------

df_Sel.groupBy('purpose').count().show()

# COMMAND ----------

df_Sel.groupBy('purpose').count().orderBy(col('count').desc()).show()

# COMMAND ----------

quantileProbs =[0.25, 0.50 , 0.75, 0.90]
relError = 0.05
df_Sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)

# COMMAND ----------

from pyspark.sql.functions import isnan, when , count, col 
df_Sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_Sel.columns]).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_status , count(*) from loanstats group by loan_status order by 2 desc;

# COMMAND ----------

df_Sel = df_Sel.na.drop('all', subset=['loan_status'])

# COMMAND ----------

df_Sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_Sel.columns]).display()

# COMMAND ----------

df_Sel.count()

# COMMAND ----------

df_Sel.describe('dti','revol_util').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select ceil(regexp_replace(revol_util, '\%','' )), count(*) from loanstatus_Sel group by ceil(regexp_replace(revol_util, '\%','' ));

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstatus_Sel where revol_util is null;

# COMMAND ----------

df_Sel = df_Sel.withColumn('revol_util_cleaned', regexp_extract('revol_util','\\d+',0))

# COMMAND ----------

df_Sel.describe('revol_util','revol_util_cleaned').show()

# COMMAND ----------

# MAGIC %md
# MAGIC stddev and and mean is showinf null because it is string value before but max value is still 99, but when u check the data u can see values like 185 which is larger than 99, this is becuase the column i sstill a string

# COMMAND ----------

from pyspark.sql.functions import avg
def fill_avg(df, colname):
    return df.select(colname).agg(avg(colname))

# COMMAND ----------


rev_avg = fill_avg(df_Sel, 'revol_util_cleaned')


# COMMAND ----------

from pyspark.sql.functions import lit
rev_avg = fill_avg(df_Sel, 'revol_util_cleaned').first()[0]
df_Sel = df_Sel.withColumn('rev_Avg',lit(rev_avg))


# COMMAND ----------

from pyspark.sql.functions import coalesce
df_Sel = df_Sel.withColumn('revol_util_cleaned',coalesce(col('revol_util_cleaned'),col('rev_Avg')))

# COMMAND ----------

df_Sel.display()

# COMMAND ----------

df_Sel = df_Sel.describe('revol_util', 'revol_util_cleaned').show()

# COMMAND ----------

df_Sel = df_Sel.withColumn('revol_util_cleaned',col('revol_util_cleaned').cast('double'))

# COMMAND ----------

df_Sel.describe('revol_util','revol_util_cleaned').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanStatus_Sel where dti is Null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select application_type, dti, dti_joint from loanStatus_Sel where dti is Null;

# COMMAND ----------

df_Sel = df_Sel.withColumn('dti_cleaned', coalesce(col('dti'),col('dti_joint')))

# COMMAND ----------

df_Sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_Sel.columns]).display()

# COMMAND ----------

df_Sel.groupBy('loan_status').count().display()

# COMMAND ----------

df_Sel.where(df_Sel.loan_status.isin(['Late (31-120 days)','In Grace Period','Charged Off','Late (16-30 days)'])).show()

# COMMAND ----------

df_Sel = df_Sel.withColumn('bad_loan', when(df_Sel.loan_status.isin(['Late (31-120 days)','In Grace Period','Charged Off','Late (16-30 days)']),'Yes').otherwise('No'))

# COMMAND ----------

df_Sel.groupBy('bad_loan').count().show()

# COMMAND ----------

df_Sel.filter(df_Sel.bad_loan == 'Yes').show()

# COMMAND ----------

def_sel_final = df_Sel.drop('revol_util','dti','dti_joint')

# COMMAND ----------

def_sel_final.display()

# COMMAND ----------

df_Sel.stat.crosstab('bad_loan','grade').show()

# COMMAND ----------

df_Sel.describe('dti_cleaned').show()

# COMMAND ----------

df_Sel.filter(df_Sel.dti_cleaned > 100).show()

# COMMAND ----------

permanent_table_name = 'lc_loan_data'
df_Sel.write.format('parquet').saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lc_loan_data;

# COMMAND ----------

