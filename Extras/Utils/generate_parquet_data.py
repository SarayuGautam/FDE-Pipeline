#!/usr/bin/env python3
"""
Data Generation Script for Product Categories Parquet Files
This script generates sample data for the parquet integration assignment.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_product_categories_data(num_categories=50):
    """Generate product category hierarchy data"""

    # Define main category types
    main_categories = [
        'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors',
        'Books & Media', 'Automotive', 'Health & Beauty', 'Toys & Games',
        'Food & Beverages', 'Jewelry & Watches'
    ]

    categories_data = []
    category_id_counter = 1

    # Generate main categories (level 1)
    for main_cat in main_categories:
        category = {
            'category_id': f'CAT{category_id_counter:03d}',
            'category_name': main_cat,
            'category_description': f'Main category for {main_cat.lower()} products',
            'parent_category_id': None,
            'category_level': 1,
            'sort_order': category_id_counter,
            'is_active': True,
            'created_date': datetime.now() - timedelta(days=np.random.randint(1, 1000)),
            'last_updated': datetime.now(),
            'category_path': f'/{main_cat}',
            'category_depth': 1
        }
        categories_data.append(category)
        category_id_counter += 1

    # Generate subcategories (level 2)
    subcategory_mappings = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Audio', 'Cameras', 'Gaming'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories', 'Shoes', 'Activewear'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Bathroom', 'Garden', 'Lighting', 'Decor'],
        'Sports & Outdoors': ['Fitness', 'Outdoor Recreation', 'Team Sports', 'Water Sports', 'Winter Sports'],
        'Books & Media': ['Fiction', 'Non-Fiction', 'Children\'s Books', 'Audio Books', 'E-Books', 'Magazines'],
        'Automotive': ['Cars', 'Motorcycles', 'Parts', 'Accessories', 'Tools', 'Maintenance'],
        'Health & Beauty': ['Skincare', 'Makeup', 'Hair Care', 'Personal Care', 'Vitamins', 'Fitness'],
        'Toys & Games': ['Board Games', 'Video Games', 'Educational Toys', 'Outdoor Toys', 'Collectibles'],
        'Food & Beverages': ['Fresh Food', 'Pantry', 'Beverages', 'Snacks', 'Organic', 'International'],
        'Jewelry & Watches': ['Necklaces', 'Earrings', 'Rings', 'Bracelets', 'Watches', 'Luxury'
        ]
    }

    for main_cat, subcats in subcategory_mappings.items():
        main_cat_id = next(cat['category_id'] for cat in categories_data if cat['category_name'] == main_cat)

        for subcat in subcats:
            category = {
                'category_id': f'CAT{category_id_counter:03d}',
                'category_name': subcat,
                'category_description': f'{subcat} products in {main_cat.lower()} category',
                'parent_category_id': main_cat_id,
                'category_level': 2,
                'sort_order': category_id_counter,
                'is_active': True,
                'created_date': datetime.now() - timedelta(days=np.random.randint(1, 500)),
                'last_updated': datetime.now(),
                'category_path': f'/{main_cat}/{subcat}',
                'category_depth': 2
            }
            categories_data.append(category)
            category_id_counter += 1

    # Generate sub-subcategories (level 3) for some popular categories
    sub_subcategory_mappings = {
        'Smartphones': ['Android Phones', 'iPhones', 'Budget Phones', 'Flagship Phones'],
        'Laptops': ['Gaming Laptops', 'Business Laptops', 'Student Laptops', 'Ultrabooks'],
        'Men': ['Shirts', 'Pants', 'Jackets', 'Suits', 'Casual Wear'],
        'Women': ['Dresses', 'Tops', 'Bottoms', 'Outerwear', 'Formal Wear'],
        'Furniture': ['Living Room', 'Bedroom', 'Dining Room', 'Office', 'Outdoor'],
        'Kitchen': ['Appliances', 'Cookware', 'Utensils', 'Storage', 'Small Appliances']
    }

    for subcat, sub_subcats in sub_subcategory_mappings.items():
        subcat_id = next(cat['category_id'] for cat in categories_data if cat['category_name'] == subcat)

        for sub_subcat in sub_subcats:
            category = {
                'category_id': f'CAT{category_id_counter:03d}',
                'category_name': sub_subcat,
                'category_description': f'{sub_subcat} in {subcat.lower()} category',
                'parent_category_id': subcat_id,
                'category_level': 3,
                'sort_order': category_id_counter,
                'is_active': True,
                'created_date': datetime.now() - timedelta(days=np.random.randint(1, 200)),
                'last_updated': datetime.now(),
                'category_path': f'/{subcat}/{sub_subcat}',
                'category_depth': 3
            }
            categories_data.append(category)
            category_id_counter += 1

    # Add some inactive categories for testing
    inactive_categories = [
        'Discontinued Electronics',
        'Seasonal Clothing',
        'Limited Edition Items'
    ]

    for inactive_cat in inactive_categories:
        category = {
            'category_id': f'CAT{category_id_counter:03d}',
            'category_name': inactive_cat,
            'category_description': f'Inactive category: {inactive_cat.lower()}',
            'parent_category_id': None,
            'category_level': 1,
            'sort_order': category_id_counter,
            'is_active': False,
            'created_date': datetime.now() - timedelta(days=np.random.randint(100, 1000)),
            'last_updated': datetime.now() - timedelta(days=np.random.randint(1, 100)),
            'category_path': f'/{inactive_cat}',
            'category_depth': 1
        }
        categories_data.append(category)
        category_id_counter += 1

    return pd.DataFrame(categories_data)

def main():
    """Main function to generate and save parquet files"""

    print("Generating sample parquet data for the assignment...")

    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Generate product categories data
    print("Generating product categories data...")
    categories_df = generate_product_categories_data(50)
    categories_file = os.path.join(data_dir, 'product_categories.parquet')
    categories_df.to_parquet(categories_file, index=False)
    print(f"Product categories data saved to: {categories_file}")
    print(f"Generated {len(categories_df)} category records")

    # Display sample data
    print("\n=== Sample Product Categories Data ===")
    print(categories_df.head(10))
    print(f"\nCategories data shape: {categories_df.shape}")

    # Display category hierarchy summary
    print("\n=== Category Hierarchy Summary ===")
    level_counts = categories_df['category_level'].value_counts().sort_index()
    for level, count in level_counts.items():
        print(f"Level {level}: {count} categories")

    active_count = categories_df['is_active'].sum()
    inactive_count = len(categories_df) - active_count
    print(f"Active categories: {active_count}")
    print(f"Inactive categories: {inactive_count}")

    # Display data types
    print("\n=== Categories Data Types ===")
    print(categories_df.dtypes)

    # Show some sample category paths
    print("\n=== Sample Category Paths ===")
    sample_paths = categories_df[['category_name', 'category_path', 'category_level']].head(15)
    print(sample_paths.to_string(index=False))

    print("\nData generation completed successfully!")
    print(f"Files saved in: {data_dir}")

if __name__ == "__main__":
    main()
