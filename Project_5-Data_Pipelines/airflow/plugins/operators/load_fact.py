from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table='songplays',
                 redshift_conn_id='redshift',
                 columns='',
                 insert_mode='append',
                 insert_sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.insert_mode=insert_mode
        self.insert_sql=insert_sql
        self.columns = columns
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.insert_mode == 'truncate':
            self.log.info('deleting data from {} table...'.format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Loading fact table: {}'.format(self.table))
        redshift_hook.run('INSERT INTO {} ({}) {}'.format(self.table, self.columns, self.insert_sql))
        
