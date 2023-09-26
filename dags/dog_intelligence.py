"""
dog_intelligence
DAG auto-generated by Astro Cloud IDE.
"""

from airflow.decorators import dag
from astro import sql as aql
from astro.table import Table, Metadata
import pandas as pd
import pendulum


@aql.run_raw_sql(conn_id="snowflake_conn", task_id="query_table", results_format="pandas_dataframe")
def query_table_func():
    return """
    SELECT * FROM SANDBOX.TAMARAFINGERLIN.DOG_INTELLIGENCE 
    WHERE CONCAT(BREED, HEIGHT_LOW_INCHES, HEIGHT_HIGH_INCHES, WEIGHT_LOW_LBS, 
    WEIGHT_HIGH_LBS, REPS_UPPER, REPS_LOWER) IS NOT NULL
    
    """

@aql.run_raw_sql(conn_id="snowflake_conn", task_id="transform_table", results_format="pandas_dataframe")
def transform_table_func(query_table: Table):
    return """
    SELECT HEIGHT_LOW_INCHES, HEIGHT_HIGH_INCHES, WEIGHT_LOW_LBS, WEIGHT_HIGH_LBS,
        CASE WHEN reps_upper <= 25 THEN 'very_smart_dog'
        ELSE 'smart_dog'
        END AS INTELLIGENCE_CATEGORY
    FROM {{query_table}}
    """

@aql.dataframe(task_id="model_task")
def model_task_func(transform_table: pd.DataFrame):
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestClassifier
    
    # use the table returned from the transform_table cell
    df = transform_table
    
    # calculate baseline accuracy
    baseline_accuracy = df.iloc[:,-1].value_counts(normalize=True)[0]
    
    # selecting predictors (X) and the target (y)
    X = df.iloc[:,:-1]
    y = df.iloc[:,-1]
    
    # split the data into training data (80%) and testing data (20%)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=23
    )
    
    # standardize features
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)
    
    # train a RandomForestClassifier on the training data
    model = RandomForestClassifier(max_depth=3, random_state=19)
    model.fit(X_train_s, y_train)
    
    # score the trained model on the testing data
    score = model.score(X_test_s, y_test)
    
    # get feature importances
    feature_importances = list(zip(X_train.columns, model.feature_importances_))
    
    return f"""
    baseline accuracy: {baseline_accuracy},\n
    model accuracy: {score},\n
    feature importances: {feature_importances}
    """ 

default_args={
    "owner": "Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.from_format("2023-09-26", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Open in Cloud IDE": "https://cloud.astronomer.io/clmamxzds00md01mw0027nnz4/cloud-ide/cln0pl916004m01mj5lqm3d2u/cln0pq3rv004t01lkb3ny98tw",
    },
)
def dog_intelligence():
    query_table = query_table_func()

    transform_table = transform_table_func(
        query_table,
    )

    model_task = model_task_func(
        transform_table,
    )

dag_obj = dog_intelligence()
