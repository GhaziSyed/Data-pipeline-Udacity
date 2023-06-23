from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    # SQL query for f-string execution
    insert_sql = """
        INSERT INTO {}
        {};
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    # Provide default values for the method
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):
    # Instantiate with user defined values
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table
    # Execute the operator's functionality      
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))

        self.log.info(f"Loading dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        
        self.log.info(f"Executing Query: {formatted_sql}")
        redshift.run(formatted_sql)
