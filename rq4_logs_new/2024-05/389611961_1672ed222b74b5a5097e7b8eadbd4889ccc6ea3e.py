import os
import subprocess
from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = 'Import production database'

    def handle(self, *args, **options):
        # Define the shell commands
        # psql_cmd = f'psql {settings.LOCAL_DB_URI} < dump.sql'
        install_pg_client_cmd = f"""
        sudo apt install curl ca-certificates
        sudo install -d /usr/share/postgresql-common/pgdg
        sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
        sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
        sudo apt update 
        sudo apt install -y postgresql-client-16
        """
        pg_dump_cmd = f"pg_dump '{settings.PROD_DB_READONLY_URI}' -cxO --no-acl -f dump.sql"
        psql_cmd = f'echo "psql {os.getenv("DATABASE_URL")} < dump.sql"'

        # Run install_pg_client_cmd command
        subprocess.run(install_pg_client_cmd, shell=True, check=True)

        # Run pg_dump command
        subprocess.run(pg_dump_cmd, shell=True, check=True)

        # Run psql command
        subprocess.run(psql_cmd, shell=True, check=True)

        # Clean up the dump file
        subprocess.run('rm dump.sql', shell=True, check=True)

        self.stdout.write(self.style.SUCCESS(
            'Database imported successfully!'))