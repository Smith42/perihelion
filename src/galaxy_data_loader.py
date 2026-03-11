"""Load galaxy data from HuggingFace datasets and update profiles."""

import os
import logging
from typing import Dict, Any
import requests
from PIL import Image
import io

try:
    from datasets import load_dataset
    DATASETS_AVAILABLE = True
except ImportError:
    DATASETS_AVAILABLE = False
    logging.warning("datasets library not available, using fallback data")

logger = logging.getLogger(__name__)

def load_galaxy_data() -> Dict[str, Dict[str, Any]]:
    """Load galaxy data from HuggingFace datasets and return structured profiles."""
    
    if not DATASETS_AVAILABLE:
        logger.warning("Using fallback galaxy data")
        return get_fallback_galaxy_data()
    
    try:
        # Load both datasets
        euclid_dataset = load_dataset("mwalmsley/gz_euclid", "tiny", split="test")
        descriptions_dataset = load_dataset("Smith42/dating_pool_but_galaxies", split="train")
        
        # Convert to dictionaries for easier access
        euclid_data = {row['id_str']: row for row in euclid_dataset}
        descriptions_data = {row['id_str']: row for row in descriptions_dataset}
        
        galaxy_profiles = {}
        
        # Match on id_str and create profiles with numbered galaxy IDs
        matched_galaxies = []
        for galaxy_id_str in euclid_data.keys():
            if galaxy_id_str in descriptions_data:
                matched_galaxies.append(galaxy_id_str)
        
        # Sort for consistent ordering
        matched_galaxies.sort()
        
        # Create numbered galaxy profiles (galaxy_01, galaxy_02, etc.)
        for i, galaxy_id_str in enumerate(matched_galaxies, 1):
            numbered_id = f"galaxy_{i:02d}"
            euclid_row = euclid_data[galaxy_id_str]
            desc_row = descriptions_data[galaxy_id_str]
            
            # Extract name and description from caption
            caption = desc_row.get('caption', '')
            bio = ""
            description = ""
            name = f"Galaxy {i}"
            
            # Parse the caption to extract bio and name if available
            if caption:
                lines = caption.split('\\n')
                if lines and lines[0].startswith('Bio:'):
                    bio_part = lines[0].replace('Bio:', '').strip()
                    bio = bio_part[:100] + "..." if len(bio_part) > 100 else bio_part
                    description = bio_part
                else:
                    description = caption.strip()
                    bio = description[:100] + "..." if len(description) > 100 else description
            
            galaxy_profiles[numbered_id] = {
                "name": name,
                "bio": bio,
                "description": description,  
                "tags": ["Cosmic", "Mysterious"],
                "color": generate_color_from_id(galaxy_id_str),
                "id_str": galaxy_id_str,
                "image_data": euclid_row.get('image'),  # PIL Image object
                "euclid_features": {k: v for k, v in euclid_row.items() if k not in ['image', 'id_str']}
            }
        
        logger.info(f"Loaded {len(galaxy_profiles)} galaxy profiles from HuggingFace datasets")
        return galaxy_profiles
        
    except Exception as e:
        logger.error(f"Error loading from HuggingFace datasets: {e}")
        return get_fallback_galaxy_data()

def generate_color_from_id(id_str: str) -> str:
    """Generate a consistent color from galaxy ID string."""
    colors = [
        "#A688C9", "#D68B8B", "#E8A0D0", "#7BC9A0", "#E87D5A",
        "#6B8DD6", "#D4A76A", "#C9A688", "#B8A0C9", "#A0D4E8",
        "#E8D4A0", "#A0E8B8", "#E8A0A0", "#A0A0E8", "#E8E8A0",
        "#D0A0E8", "#A0E8D0", "#E8C4A0", "#C4A0E8", "#A0C4E8",
        "#E8A0C4", "#B4E8A0", "#A0E8E8"
    ]
    # Use hash to get consistent color index
    hash_val = sum(ord(c) for c in id_str)
    return colors[hash_val % len(colors)]

def get_fallback_galaxy_data() -> Dict[str, Dict[str, Any]]:
    """Fallback galaxy data using original profiles with numbered IDs."""
    fallback_profiles = {
        "galaxy_01": {
            "name": "Velvet Vortex",
            "bio": "Smooth operator with a soft spiral glow.",
            "description": "I keep my arms tight and my luminosity low-key. Looking for someone who appreciates subtlety over spectacle.",
            "tags": ["Smooth Spiral", "Low-Key", "Gentle Glow"],
            "color": "#A688C9",
            "image_url": None,
            "image_data": None
        },
        "galaxy_02": {
            "name": "Crimson Drift", 
            "bio": "Redshifted and proud.",
            "description": "I have been expanding away from everyone for billions of years and I am NOT slowing down. Commitment-phobic? Maybe. Mysterious? Definitely.",
            "tags": ["High Redshift", "Distant", "Loner Vibes"],
            "color": "#D68B8B",
            "image_url": None,
            "image_data": None
        }
        # Add more as needed...
    }
    
    # Generate numbered galaxy profiles if we have fewer than 23
    for i in range(1, 24):
        galaxy_id = f"galaxy_{i:02d}"
        if galaxy_id not in fallback_profiles:
            fallback_profiles[galaxy_id] = {
                "name": f"Galaxy {i}",
                "bio": "A mysterious galaxy in the cosmic dating scene.",
                "description": "This galaxy is looking for its perfect cosmic match.",
                "tags": ["Mysterious", "Cosmic"],
                "color": "#A688C9",
                "image_url": None,
                "image_data": None
            }
    
    return fallback_profiles

def download_galaxy_image(image_data, galaxy_id: str, images_dir: str = "images") -> bool:
    """Download and save a galaxy image from HuggingFace dataset."""
    if not image_data:
        return False
    
    try:
        # Ensure images directory exists
        os.makedirs(images_dir, exist_ok=True)
        
        # If image_data is a PIL Image (from datasets)
        if hasattr(image_data, 'save') and hasattr(image_data, 'mode'):
            image_path = os.path.join(images_dir, f"{galaxy_id}.jpg")
            # Convert to RGB if needed (in case it's RGBA or other format)
            if image_data.mode != 'RGB':
                image_data = image_data.convert('RGB')
            image_data.save(image_path, 'JPEG', quality=85)
            logger.info(f"Saved image for {galaxy_id}")
            return True
            
        # If image_data has a URL (fallback)
        elif isinstance(image_data, dict) and 'url' in image_data:
            response = requests.get(image_data['url'])
            if response.status_code == 200:
                image = Image.open(io.BytesIO(response.content))
                if image.mode != 'RGB':
                    image = image.convert('RGB')
                image_path = os.path.join(images_dir, f"{galaxy_id}.jpg")
                image.save(image_path, 'JPEG', quality=85)
                logger.info(f"Downloaded and saved image for {galaxy_id}")
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Error downloading image for {galaxy_id}: {e}")
        return False

def update_galaxy_images():
    """Download all galaxy images from the dataset."""
    galaxy_data = load_galaxy_data()
    
    for galaxy_id, profile in galaxy_data.items():
        if profile.get('image_data'):
            download_galaxy_image(profile['image_data'], galaxy_id)
        elif profile.get('image_url'):
            # Try to download from URL
            try:
                response = requests.get(profile['image_url'])
                if response.status_code == 200:
                    image = Image.open(io.BytesIO(response.content))
                    download_galaxy_image(image, galaxy_id)
            except Exception as e:
                logger.error(f"Error downloading from URL for {galaxy_id}: {e}")

if __name__ == "__main__":
    # Test the data loading
    data = load_galaxy_data()
    print(f"Loaded {len(data)} galaxies")
    for galaxy_id, profile in list(data.items())[:3]:
        print(f"{galaxy_id}: {profile['name']} - {profile['bio'][:50]}...")
    
    # Update images
    update_galaxy_images()