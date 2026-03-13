# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "datasets",
#     "google-genai",
#     "pillow",
# ]
# ///

import os
from datasets import load_dataset, concatenate_datasets
from itertools import chain
from multiprocessing import Pool
from google import genai
from functools import partial
from tqdm import tqdm
import json
from PIL import Image
import io
import math

# Configure Google API key
client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

def create_galaxy_prompt(example):
    """
    Convert galaxy metadata to a comprehensive structured prompt for an LLM.
    
    Args:
        example: Dictionary containing galaxy metadata
    
    Returns:
        str: Formatted prompt with all relevant galaxy information
    """
    # Base prompt
    base_prompt = """
    You are a galaxy on a dating app. Based on the image and your stats below, write a short, witty Tinder-style bio for this galaxy. Include flirty references to your physical features (arms, bulge, shape, etc.) and any quirks from the data. Keep it to 2-3 sentences max. Be playful and creative.

Here are your stats:
"""
    
    # Helper function to format values appropriately
    def format_value(key, value):
        # Skip None values, PIL image objects, and index values
        if value is None or str(type(value)).find('PIL') >= 0 or key == '__index_level_0__':
            return None
            
        # Handle numeric values
        if isinstance(value, (int, float)):
            return f"{value:.4f}" if isinstance(value, float) else f"{value}"
            
        # String values
        return value
    
    # Group metadata into categories
    categories = {
      "Smooth or Featured": [
        "smooth-or-featured-euclid_smooth_fraction",
        "smooth-or-featured-euclid_featured-or-disk_fraction",
        "smooth-or-featured-euclid_problem_fraction"
      ],
    
      "Disk Edge-On": [
        "disk-edge-on-euclid_yes_fraction",
        "disk-edge-on-euclid_no_fraction"
      ],
    
      "Spiral Arms": [
        "has-spiral-arms-euclid_yes_fraction",
        "has-spiral-arms-euclid_no_fraction"
      ],
    
      "Bar": [
        "bar-euclid_strong_fraction",
        "bar-euclid_weak_fraction",
        "bar-euclid_no_fraction"
      ],
    
      "Bulge Size": [
        "bulge-size-euclid_dominant_fraction",
        "bulge-size-euclid_large_fraction",
        "bulge-size-euclid_moderate_fraction",
        "bulge-size-euclid_small_fraction",
        "bulge-size-euclid_none_fraction"
      ],
    
      "How Rounded": [
        "how-rounded-euclid_round_fraction",
        "how-rounded-euclid_in-between_fraction",
        "how-rounded-euclid_cigar-shaped_fraction"
      ],
    
      "Edge-On Bulge": [
        "edge-on-bulge-euclid_boxy_fraction",
        "edge-on-bulge-euclid_none_fraction",
        "edge-on-bulge-euclid_rounded_fraction"
      ],
    
      "Spiral Winding": [
        "spiral-winding-euclid_tight_fraction",
        "spiral-winding-euclid_medium_fraction",
        "spiral-winding-euclid_loose_fraction"
      ],
    
      "Spiral Arm Count": [
        "spiral-arm-count-euclid_1_fraction",
        "spiral-arm-count-euclid_2_fraction",
        "spiral-arm-count-euclid_3_fraction",
        "spiral-arm-count-euclid_4_fraction",
        "spiral-arm-count-euclid_more-than-4_fraction",
        "spiral-arm-count-euclid_cant-tell_fraction"
      ],
    
      "Merging": [
        "merging-euclid_none_fraction",
        "merging-euclid_minor-disturbance_fraction",
        "merging-euclid_major-disturbance_fraction",
        "merging-euclid_merger_fraction"
      ],
    
      "Clumps": [
        "clumps-euclid_yes_fraction",
        "clumps-euclid_no_fraction"
      ],
    
      "Problem": [
        "problem-euclid_star_fraction",
        "problem-euclid_artifact_fraction",
        "problem-euclid_zoom_fraction"
      ],
    
      "Artifact": [
        "artifact-euclid_satellite_fraction",
        "artifact-euclid_scattered_fraction",
        "artifact-euclid_diffraction_fraction",
        "artifact-euclid_ray_fraction",
        "artifact-euclid_saturation_fraction",
        "artifact-euclid_other_fraction",
        "artifact-euclid_ghost_fraction"
      ]
    }
    
    # Build the prompt with organized scientific data
    formatted_prompt = base_prompt
    
    for category, keys in categories.items():
        # Check if we have any values for this category
        category_values = {}
        for key in keys:
            if key in example and example[key] is not None and not (isinstance(example[key], float) and math.isnan(example[key])):
                formatted_value = format_value(key, example[key])
                if formatted_value is not None:
                    category_values[key] = formatted_value
        
        # If we have values, add the category
        if category_values:
            formatted_prompt += f"\n\n{category}:"
            for key, value in category_values.items():
                # Format key for better readability
                display_key = key.replace('_', ' ').replace('-', ' ')
                display_key = ' '.join(word.capitalize() for word in display_key.split())
                formatted_prompt += f"\n- {display_key}: {value}"
    
    # Add a final instruction
    formatted_prompt += """

Based on this information and what you see in the image, write the galaxy's dating profile bio. Don't reference the raw numbers — just use them to inform your personality and pickup lines. Respond only with the caption, nothing else. Start your answer with 'Bio:'
"""
    
    return formatted_prompt


def generate_galaxy_name(image, information):
    """Generate a creative dating-app display name for a galaxy using Gemini Flash 2.0.

    Inspects the galaxy's morphological metadata to pick out standout traits,
    then asks Gemini to mint a short, memorable profile name.

    Args:
        image: PIL Image of the galaxy.
        information: Dictionary containing galaxy metadata.

    Returns:
        str: A 1-3 word dating-style display name.
    """
    # Collect notable morphological traits to steer the name
    traits = []

    def _above(key, threshold=0.5):
        val = information.get(key)
        return val is not None and not (isinstance(val, float) and math.isnan(val)) and val > threshold

    if _above("has-spiral-arms-euclid_yes_fraction", 0.5):
        traits.append("spiral arms")
    if _above("bar-euclid_strong_fraction", 0.3):
        traits.append("strong bar")
    if _above("merging-euclid_merger_fraction", 0.3):
        traits.append("currently merging")
    if _above("how-rounded-euclid_cigar-shaped_fraction", 0.3):
        traits.append("cigar-shaped")
    if _above("bulge-size-euclid_dominant_fraction", 0.3):
        traits.append("dominant bulge")
    if _above("smooth-or-featured-euclid_smooth_fraction", 0.7):
        traits.append("very smooth")
    if _above("disk-edge-on-euclid_yes_fraction", 0.5):
        traits.append("edge-on disk")
    if _above("spiral-winding-euclid_tight_fraction", 0.5):
        traits.append("tightly wound spirals")
    if _above("spiral-winding-euclid_loose_fraction", 0.5):
        traits.append("loosely wound spirals")
    if _above("clumps-euclid_yes_fraction", 0.5):
        traits.append("clumpy")

    trait_hint = (
        f" This galaxy has notable traits: {', '.join(traits)}."
        if traits
        else ""
    )

    prompt = (
        "You are naming a galaxy for its dating app profile. Give it a short, "
        "memorable, flirty display name (1-3 words max) that sounds like a fun "
        "username or nickname. It should hint at the galaxy's appearance or "
        f"personality.{trait_hint}\n\n"
        "Examples of good names: \"Spiral Daddy\", \"Thicc Bulge\", \"Arms4Days\", "
        "\"Smooth Operator\", \"Merger Maven\", \"Bar Star\", \"Edge Lord\", "
        "\"Clumpy Boi\", \"Tightly Wound\"\n\n"
        "Respond with ONLY the name, nothing else."
    )

    response = client.models.generate_content(
        contents=[prompt, image],
        model="gemini-3-flash-preview",
    )

    return response.text.strip().strip('"').strip("'")


def caption_image(image, information):
    """Generate caption for an image using Gemini Flash 2.0"""
    # Prepare the prompt
    prompt = create_galaxy_prompt(information)

    # Generate response from Gemini
    response = client.models.generate_content(
        contents=[prompt, image],
        model="gemini-3-flash-preview",
    )

    return response.text

def process_example(example):
    """Process a single example in parallel"""
    try:
        img = example['image']
        if isinstance(img, Image.Image):
            image = img
        elif isinstance(img, dict) and 'bytes' in img:
            image = Image.open(io.BytesIO(img['bytes']))
        else:
            image = Image.open(io.BytesIO(img))

        image_id = example['id_str']
        name = generate_galaxy_name(image, example)
        caption = caption_image(image, example)

        return {
            'id_str': image_id,
            'name': name,
            'caption': caption
        }
    except Exception as e:
        print(f"Error processing image {example.get('id_str', 'unknown')}: {str(e)}")
        return {
            'id_str': example['id_str'],
            'name': None,
            'caption': None
        }

def save_results(results, filename):
    """Save captioning results to JSON file"""
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)

def main():
    # Check for existing results
    split = "test"
    checkpoint_file = f"galaxy_caption_{split}_partial.json"
    results = []
    completed_count = 0
    
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                results = json.load(f)
                completed_count = len(results)
                print(f"Loaded {completed_count} existing results. Resuming...")
        except:
            print("Couldn't load checkpoint. Starting fresh.")
    
    # Load datasets and use skip() to efficiently skip processed examples
    galaxies = load_dataset("mwalmsley/gz_euclid", "tiny", split=split, streaming=True)
 
    # Skip already processed examples using HF's skip() method
    if completed_count > 0:
        galaxies = galaxies.skip(completed_count)
    
    dataset = galaxies

    max_examples = dataset.info.splits[split].num_examples
    if completed_count >= max_examples:
        print("All examples already processed.")
        save_results(results, "galaxy_captions.json")
        return

    remaining_count = max_examples - completed_count
    batch_size = 8
    dataset = dataset.batch(batch_size=batch_size)
    num_processes = 1
    proc_example = partial(process_example)
    
    for i, batch in enumerate(dataset):
        # Process batch in parallel
        # We want list of dicts not dict of lists
        batch = [{k: batch[k][i] for k in batch.keys()} for i in range(len(batch[list(batch.keys())[0]]))]
        with Pool(processes=num_processes) as pool:
            batch_results = list(tqdm(
                pool.imap(proc_example, batch),
                total=len(batch),
                desc=f"Batch {i}/{(remaining_count+batch_size-1)//batch_size}"
            ))
            
        # Add batch results and save checkpoint
        results.extend(batch_results)
        save_results(results, checkpoint_file)
        print(f"Saved checkpoint with {len(results)} galaxies")

    # Save final results
    save_results(results, f"{checkpoint_file.split('_partial')[0]}.json")
    print(f"Completed captioning {len(results)} galaxy images")

if __name__ == "__main__":
    main()
