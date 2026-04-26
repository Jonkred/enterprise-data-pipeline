"""Generate sample source data for pipeline demonstration."""

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from rich.console import Console
from rich.progress import track

console = Console()


def generate_erp_data(num_records: int = 100_000, output_dir: str = "data/sources/erp") -> None:
    """Generate sample ERP transaction data."""
    os.makedirs(output_dir, exist_ok=True)
    console.print(f"[blue]Generating {num_records:,} ERP records...[/blue]")

    now = datetime.now()
    products = [f"PROD_{i:04d}" for i in range(1, 501)]
    regions = ["NORTH_AM", "SOUTH_AM", "EUROPE", "ASIA_PAC", "MIDDLE_EAST"]
    channels = ["ONLINE", "RETAIL", "WHOLESALE", "B2B"]

    # Generate in batches for memory efficiency
    batch_size = 10_000
    batches = []

    for batch_num in track(range(0, num_records, batch_size), description="ERP batches"):
        n = min(batch_size, num_records - batch_num)
        data = {
            "transaction_id": [f"TXN_{batch_num + i:010d}" for i in range(n)],
            "product_id": random.choices(products, k=n),
            "customer_id": [f"CUST_{random.randint(1, 50000):08d}" for _ in range(n)],
            "order_date": [
                (now - timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
                for _ in range(n)
            ],
            "quantity": random.choices(range(1, 101), k=n),
            "unit_price": [round(random.uniform(5.0, 999.99), 2) for _ in range(n)],
            "discount_pct": [round(random.uniform(0, 0.3), 2) for _ in range(n)],
            "region": random.choices(regions, k=n),
            "sales_channel": random.choices(channels, k=n),
            "sales_rep_id": [f"REP_{random.randint(100, 999)}" for _ in range(n)],
            "updated_at": [
                (now - timedelta(hours=random.randint(0, 168))).isoformat()
                for _ in range(n)
            ],
        }
        batches.append(pd.DataFrame(data))

    df = pd.concat(batches, ignore_index=True)
    df["amount"] = (df["quantity"] * df["unit_price"] * (1 - df["discount_pct"])).round(2)

    # Write as Parquet
    output_path = os.path.join(output_dir, "erp_transactions.parquet")
    df.to_parquet(output_path, compression="snappy", index=False)
    console.print(f"[green]✓ ERP data written: {output_path} ({len(df):,} rows)[/green]")


def generate_reference_data(output_dir: str = "data/sources/reference") -> None:
    """Generate dimension tables for enrichment."""
    os.makedirs(output_dir, exist_ok=True)

    # Geography dimension
    geo_data = {
        "zip_code": [f"{random.randint(10000, 99999):05d}" for _ in range(1000)],
        "city": random.choices(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"], k=1000),
        "state": random.choices(["NY", "CA", "IL", "TX", "AZ"], k=1000),
        "country": ["US"] * 1000,
        "latitude": [round(random.uniform(25, 48), 4) for _ in range(1000)],
        "longitude": [round(random.uniform(-124, -67), 4) for _ in range(1000)],
    }
    geo_df = pd.DataFrame(geo_data)
    geo_df.to_parquet(os.path.join(output_dir, "dim_geolocation.parquet"), index=False)

    # Product dimension
    products = []
    for i in range(1, 501):
        products.append({
            "product_id": f"PROD_{i:04d}",
            "product_name": f"Product {i}",
            "category": random.choice(["ELECTRONICS", "CLOTHING", "FOOD", "HOME", "SPORTS"]),
            "subcategory": random.choice(["A", "B", "C"]),
            "supplier_id": f"SUP_{random.randint(1, 50):03d}",
            "cost_price": round(random.uniform(2.0, 500.0), 2),
        })

    pd.DataFrame(products).to_parquet(
        os.path.join(output_dir, "dim_products.parquet"), index=False
    )

    console.print(f"[green]✓ Reference dimensions written[/green]")


if __name__ == "__main__":
    console.rule("[bold]Generating Sample Source Data")
    generate_erp_data(num_records=100_000)
    generate_reference_data()
    console.rule("[bold green]Sample data generation complete")
