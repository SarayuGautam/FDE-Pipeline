#!/usr/bin/env python3
import os
from datetime import datetime
import pandas as pd


def build_category_id(category_name: str) -> str:
    """Build a stable category_id from category name, max 20 chars, uppercase, alnum/underscore."""
    if category_name is None:
        return None
    sanitized = pd.Series([category_name]).astype(str).str.replace(r"[^a-zA-Z0-9]+", "_", regex=True).str.upper().iloc[0]
    return sanitized[:20] if sanitized else None


def main():
    base_dir = os.path.dirname(__file__)
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))

    dim_products_csv = os.path.join(data_dir, "dim_products.csv")
    fact_sales_csv = os.path.join(data_dir, "fact_sales.csv")
    output_parquet = os.path.join(data_dir, "product_categories.parquet")

    if not os.path.exists(dim_products_csv):
        raise FileNotFoundError(f"Missing dim_products.csv at {dim_products_csv}")
    if not os.path.exists(fact_sales_csv):
        raise FileNotFoundError(f"Missing fact_sales.csv at {fact_sales_csv}")

    products = pd.read_csv(dim_products_csv)
    sales = pd.read_csv(fact_sales_csv)

    # Ensure expected columns
    if "product_id" not in products.columns or "category" not in products.columns:
        raise ValueError("dim_products.csv must contain columns: product_id, category")
    if "product_id" not in sales.columns:
        raise ValueError("fact_sales.csv must contain column: product_id")

    # Filter to products that appear in sales and compute revenue per product
    sales_prod = sales.dropna(subset=["product_id"]).copy()
    sales_prod["product_id"] = sales_prod["product_id"].astype(str)
    sales_agg = sales_prod.groupby("product_id")["total_amount"].sum().reset_index()
    products["product_id"] = products["product_id"].astype(str)
    sold_products = products.merge(sales_agg, on="product_id", how="inner")

    # Derive distinct non-null categories, collapsing case/whitespace variants
    cats = sold_products[["category"]].dropna().copy()
    cats["category"] = cats["category"].astype(str).str.strip()
    cats = cats[cats["category"].str.len() > 0]

    # Build a canonical name per normalized key using most frequent original value
    cats["norm"] = cats["category"].str.lower()
    mode_by_norm = (
        cats.groupby("norm")["category"]
        .agg(lambda s: s.value_counts().idxmax())
        .to_dict()
    )
    # Rank categories by revenue
    sold_products["category_norm"] = sold_products["category"].str.lower()
    revenue_by_cat = sold_products.groupby("category_norm")["total_amount"].sum().reset_index()
    revenue_by_cat = revenue_by_cat.sort_values(["total_amount", "category_norm"], ascending=[False, True])

    # Map norm -> canonical name, and select top 10
    norm_to_name = {n: mode_by_norm[n] for n in mode_by_norm}
    top_norms = revenue_by_cat["category_norm"].head(10).tolist()
    distinct_categories = [norm_to_name[n] for n in top_norms if n in norm_to_name]

    now = datetime.now()
    rows = []
    for cat in distinct_categories:
        # Use the raw canonical category text as the ID so it matches dp.category
        cat_id = str(cat)
        if not cat_id:
            continue
        rows.append(
            {
                "category_id": cat_id,
                "category_description": f"{cat} category",
                "parent_category_id": None,
                "category_level": 1,
                "sort_order": None,
                "is_active": True,
                "created_date": now,
                "last_updated": now,
                "category_path": cat,
                "source_system": "DERIVED",
            }
        )

    # Curated level-2 only: small, reliable list per parent. No level-3.
    enrichment = {
        "ELECTRONICS": ["Smartphones", "Laptops", "Audio"],
        "BEAUTY": ["Makeup", "Skincare", "Fragrances"],
        "GROCERIES": ["Fresh Food", "Beverages", "Snacks"],
        "HOME": ["Furniture", "Kitchen", "Decor"],
        "KITCHEN": ["Appliances", "Cookware", "Utensils"],
        "OFFICE": ["Supplies", "Furniture", "Electronics"],
        "OUTDOOR": ["Recreation", "Garden", "Camping"],
        "PERSONAL CARE": ["Hygiene", "Skin Care"],
        "PETS": ["Dog", "Cat", "Other"],
        "DRINKWARE": ["Bottles", "Tumblers", "Mugs"],
        "ACCESSORIES": ["Bags", "Belts", "Hats"],
        "RECREATION": ["Fitness", "Team Sports", "Outdoor Recreation"],
        "AUDIO": ["Headphones", "Speakers"],
        "CAMERAS": ["DSLR", "Mirrorless", "Action Cams"],
        "SMARTPHONES": ["Android Phones", "iPhones"],
        "LAPTOPS": ["Gaming Laptops", "Business Laptops", "Student Laptops"],
        "TELEVISIONS": ["LED", "OLED", "QLED"],
        "BOOKS": ["Fiction", "Non-Fiction"],
        "E-READERS": ["Accessories"],
        "FRAGRANCES": ["Men", "Women", "Unisex"],
        "FURNITURE": ["Living Room", "Bedroom", "Office"],
        "FOOTWEAR": ["Running", "Casual", "Formal"],
        "WEARABLES": ["Smart Watches", "Fitness Trackers"],
        "TOYS": ["Board Games", "Video Games", "Collectibles"],
    }

    existing_ids = {r["category_id"] for r in rows}
    # Use the top-level segment of category_path as the parent display/name
    name_to_id = {str(r["category_path"]).split("/")[0].upper(): r["category_id"] for r in rows}

    for parent_upper, parent_id in list(name_to_id.items()):
        if parent_upper not in enrichment:
            continue
        # Take only the first 2 curated subs per parent to target 20 rows total
        for sub in enrichment[parent_upper][:2]:
            # Ensure uniqueness across parents by prefixing parent to the sub id
            sub_id = build_category_id(f"{parent_upper.title()}_{sub}")
            if not sub_id or sub_id in existing_ids:
                continue
            rows.append(
                {
                    "category_id": sub_id,
                    "category_description": f"{sub} under {parent_upper.title()}",
                    "parent_category_id": parent_id,
                    "category_level": 2,
                    "sort_order": None,
                    "is_active": True,
                    "created_date": now,
                    "last_updated": now,
                    "category_path": f"{parent_upper.title()}/{sub}",
                    "source_system": "DERIVED",
                }
            )
            existing_ids.add(sub_id)

    df = pd.DataFrame(rows)
    # Ensure schema order (drop name/depth/sort_order/is_active/source_system)
    df = df[
        [
            "category_id",
            "category_description",
            "parent_category_id",
            "category_level",
            "created_date",
            "last_updated",
            "category_path",
        ]
    ]

    df.to_parquet(output_parquet, index=False)
    print(f"Wrote {len(df)} categories to {output_parquet}")


if __name__ == "__main__":
    main()


