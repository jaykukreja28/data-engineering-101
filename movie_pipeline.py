import csv
from datetime import datetime, date
import json

# Sample data - will create movies.csv automatically
sample_data = """title,release_date,rating
The Shawshank Redemption,1994-09-23,9.3
The Godfather,1972-03-24,9.2
The Dark Knight,2008-07-18,9.0
Pulp Fiction,1994-10-14,8.9
Fight Club,1999-10-15,8.8"""

# Step 1: Data Loading
def load_movies(filename):
    """Load movies from CSV"""
    with open(filename, mode='r') as file:
        return list(csv.DictReader(file))

# Step 2: Type Conversion
def convert_types(movie):
    """Convert data types"""
    movie['release_date'] = datetime.strptime(movie['release_date'], '%Y-%m-%d').date()
    movie['rating'] = float(movie['rating'])
    return movie

# Step 3: Data Enrichment
def enrich_movies(movies):
    """Add derived fields"""
    for movie in movies:
        movie['decade'] = (movie['release_date'].year // 10) * 10
        movie['rating_category'] = 'Classic' if movie['rating'] >= 9.0 else 'Excellent'
        movie['years_since_release'] = date.today().year - movie['release_date'].year
    return movies

# Step 4: Analysis
def analyze_movies(movies):
    """Basic analysis"""
    print("\n=== ANALYSIS RESULTS ===")
    avg_rating = sum(m['rating'] for m in movies) / len(movies)
    print(f"Average rating: {avg_rating:.1f}/10")
    
    print("\nTop 3 Movies:")
    for i, movie in enumerate(sorted(movies, key=lambda x: x['rating'], reverse=True)[:3], 1):
        print(f"{i}. {movie['title']} ({movie['rating']})")

# Step 5: Save Results
def save_results(movies):
    """Save processed data"""
    with open('processed_movies.json', 'w') as f:
        json.dump(movies, f, indent=2, default=str)
    print("\nSaved processed data to processed_movies.json")

# Main Pipeline
def main():
    # Create sample file
    with open('movies.csv', 'w') as f:
        f.write(sample_data)
    
    print("=== MOVIE DATA PIPELINE ===")
    
    # 1. Load
    print("\n[1/3] Loading data...")
    raw_movies = load_movies('movies.csv')
    print(f"Loaded {len(raw_movies)} raw movies")
    
    # 2. Convert
    print("[2/3] Converting types...")
    converted_movies = [convert_types(m) for m in raw_movies]
    print(f"First movie after conversion: {converted_movies[0]}")
    
    # 3. Enrich
    print("[3/3] Enriching data...")
    final_movies = enrich_movies(converted_movies)
    
    # Analyze & Save
    analyze_movies(final_movies)
    save_results(final_movies)
    
    print("\nPipeline complete! Check processed_movies.json")

if __name__ == "__main__":
    main()