import os
import datetime
from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connections
from django.apps import apps

class Command(BaseCommand):
    help = 'Migrate and create log tables with triggers for all apps'

    def handle(self, *args, **kwargs):
        # Step 1: Apply regular Django migrations
        self.stdout.write("Applying migration...")
        call_command("makemigrations")

        # Step 2: Apply regular Django migrate
        self.stdout.write("Applying migrate...")
        call_command("migrate")

        # Step 3: Generate SQL for log tables and triggers
        self.stdout.write("Generating SQL for log tables and triggers...")
        sql_file_path = self.generate_sql_file()

        # Step 4: Execute the SQL file on the database
        self.execute_sql(sql_file_path)
        self.stdout.write("Migration controller execution completed.")

    def generate_sql_file(self):
        # Generate a timestamped SQL file name
        sql_file_path = os.path.join(
            os.getcwd(), f"migrate_controller_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        )
        
        # Collect SQL statements for all models across apps
        sql_statements = ""
        for model in apps.get_models():
            sql_statements += self.generate_log_table_sql(model)

        # Write the SQL statements to the file
        with open(sql_file_path, 'w') as f:
            f.write(sql_statements)

        self.stdout.write(f"SQL file generated at: {sql_file_path}")
        return sql_file_path

    def generate_log_table_sql(self, model):
        """Generate SQL for creating log tables and triggers for a given model."""
        table_name = model._meta.db_table
        log_table_name = f"log_{table_name}"
        fields = [f"`{field.column}` {field.db_type(connections['default'])}" for field in model._meta.fields]

        # Add log-specific fields
        fields.extend([
            "`log_id` INT PRIMARY KEY AUTO_INCREMENT",
            "`log_time` DATETIME NOT NULL",
            "`done_by` INT"
        ])

        # Create log table SQL
        sql = f"CREATE TABLE IF NOT EXISTS `{log_table_name}` (\n  {', '.join(fields)}\n);\n\n"

        # Create triggers for INSERT, UPDATE, DELETE
        sql += self.create_trigger_sql(table_name, log_table_name, "INSERT")
        sql += self.create_trigger_sql(table_name, log_table_name, "UPDATE")
        sql += self.create_trigger_sql(table_name, log_table_name, "DELETE")

        return sql

    def create_trigger_sql(self, table_name, log_table_name, action):
        """Generate SQL for a trigger on a specific action."""
        trigger_name = f"{action.lower()}_{table_name}_trigger"
        old_new_ref = "NEW" if action == "INSERT" else "OLD"

        # Prepare column names and values
        columns = ", ".join([f"`{field.column}`" for field in apps.get_model('your_app', table_name)._meta.fields])
        values = ", ".join([f"{old_new_ref}.`{field.column}`" for field in apps.get_model('your_app', table_name)._meta.fields])

        # Generate the trigger SQL
        return f"""
CREATE TRIGGER {trigger_name} AFTER {action} ON `{table_name}`
FOR EACH ROW
INSERT INTO `{log_table_name}` ({columns}, `log_time`, `done_by`)
VALUES ({values}, NOW(), 1);  -- Replace 1 with user ID logic if required
"""

    def execute_sql(self, sql_file_path):
        """Execute the generated SQL file on the MySQL database."""
        # pass
        with open(sql_file_path, 'r') as f:
            sql = f.read()

        # Execute the SQL using Django's default connection
        with connections['default'].cursor() as cursor:
            cursor.execute(sql)
