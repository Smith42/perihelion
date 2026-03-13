from datasets import Dataset

dataset = Dataset.from_json("galaxy_caption_test.json")
dataset.push_to_hub("your_user/dating_pool_but_galaxies")
