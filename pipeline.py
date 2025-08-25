import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from prefect import flow, task

# from Loader.categories import load_categories
from Extractor.archive import main as archive_main
from Extractor.main_extractor import MainExtractor
from Loader.products import load_products
from Loader.sales import load_sales
from Loader.users import load_users


@task(retries=3, retry_delay_seconds=60)
def extract_task():
    extractor = MainExtractor()
    extractor.extract_all()


@task(retries=2, retry_delay_seconds=30)
def load_products_task():
    load_products()


@task(retries=2, retry_delay_seconds=30)
def load_users_task():
    load_users()


@task(retries=2, retry_delay_seconds=30)
def load_sales_task():
    load_sales()


@task(retries=2, retry_delay_seconds=30)
# def load_categories_task():
#     load_categories()


@task(retries=2, retry_delay_seconds=30)
def archive_task():
    archive_main()


@flow(name="FDE Data Pipeline")
def fde_pipeline():
    extract_task()

    # Load categories first (dimension table)
    # load_categories_task()
    products_future = load_products_task.submit()
    users_future = load_users_task.submit()
    products_future.result()
    users_future.result()

    # Finally load sales and archive
    load_sales_task()
    archive_task()


if __name__ == "__main__":
    fde_pipeline()
