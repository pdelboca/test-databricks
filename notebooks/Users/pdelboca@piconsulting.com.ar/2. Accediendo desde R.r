# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Accediendo desde R con SparkR

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

library(SparkR)
irisDF <- read.df("/mnt/azure-dls/sandbox/pdelboca/iris.csv", source = "csv", header="true", inferSchema = "true")
colnames(irisDF) <- c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species" )

# COMMAND ----------

head(irisDF)

# COMMAND ----------

printSchema(irisDF)

# COMMAND ----------

display(irisDF)

# COMMAND ----------

head(filter(irisDF, irisDF$Species == "setosa"))

# COMMAND ----------

head(count(groupBy(irisDF, irisDF$Species)))

# COMMAND ----------

# Fit a linear model over the dataset.
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = irisDF, family = "gaussian")

# Model coefficients are returned in a similar format to R's native glm().
summary(model)