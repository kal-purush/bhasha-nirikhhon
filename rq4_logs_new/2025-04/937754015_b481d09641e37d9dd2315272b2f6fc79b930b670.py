"""
Embedding Generator Script - Second step in PubMed processing pipeline

This script generates vector embeddings for PubMed text chunks using
sentence transformers.

Usage:
    python 2_embedding_generator.py --input /path/to/processed --output /path/to/embeddings [--limit 20] [--model model_name]
"""

import os
import sys
import json
import logging
import glob
import argparse
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm
import torch
from sentence_transformers import SentenceTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pubmed_embedding.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("pubmed_embedding")

class EmbeddingGenerator:
    def __init__(self, config: Dict = None):
        """Initialize the embedding generator with the specified model."""
        self.config = {
            "model_name": "pritamdeka/S-PubMedBERT-MS-MARCO",
            "batch_size": 32,
            "output_dir": "./processed_with_embeddings",
            "no_save": False
        }
        if config:
            self.config.update(config)
        
        # Create output directory if needed
        if "output_dir" in self.config and not self.config.get("no_save", False):
            os.makedirs(self.config["output_dir"], exist_ok=True)
            logger.info(f"Output directory: {self.config['output_dir']}")
        
        # Load the embedding model
        logger.info(f"Loading embedding model: {self.config['model_name']}")
        self.model = SentenceTransformer(self.config["model_name"])
        
        # Enable GPU if available
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        logger.info(f"Using device: {self.device}")
        
        # Get embedding dimension for verification
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        logger.info(f"Embedding dimension: {self.embedding_dim}")

    def generate_embeddings_for_file(self, file_path: str) -> Optional[Dict]:
        """Generate embeddings for all chunks in a processed file."""
        logger.info(f"Generating embeddings for {file_path}")
        
        try:
            # Load the processed document
            with open(file_path, 'r', encoding='utf-8') as f:
                document = json.load(f)
            
            # Extract chunks
            chunks = document.get('chunks', [])
            if not chunks:
                logger.warning(f"No chunks found in {file_path}")
                return document
            
            # Prepare text for embedding
            texts = [chunk['text'] for chunk in chunks]
            
            # Log some stats about the chunks
            logger.info(f"Found {len(texts)} chunks to embed")
            if texts:
                logger.info(f"Average chunk length: {sum(len(t) for t in texts) / len(texts):.1f} characters")
                logger.info(f"First chunk sample: {texts[0][:100]}...")
            
            # Generate embeddings in batches
            all_embeddings = []
            batch_size = self.config["batch_size"]
            
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i + batch_size]
                
                # Generate embeddings
                batch_embeddings = self.model.encode(batch_texts, convert_to_tensor=True)
                
                # Convert to list for JSON serialization
                all_embeddings.extend(batch_embeddings.cpu().numpy().tolist())
            
            # Add embeddings to chunks
            for i, chunk in enumerate(chunks):
                chunk['embedding'] = all_embeddings[i]
            
            # Log embedding info
            if all_embeddings:
                emb_array = np.array(all_embeddings)
                logger.info(f"Embedding shape: {emb_array.shape}")
                logger.info(f"Embedding stats: min={emb_array.min():.4f}, max={emb_array.max():.4f}, mean={emb_array.mean():.4f}")
            
            # Update the document
            document['chunks'] = chunks
            return document
            
        except Exception as e:
            logger.error(f"Error generating embeddings for {file_path}: {str(e)}", exc_info=True)
            return None

    def save_document(self, document: Dict, output_path: str) -> str:
        """Save the document with embeddings."""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(document, f, ensure_ascii=False)
            logger.info(f"Saved document with embeddings to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving document to {output_path}: {str(e)}")
            return ""

    def process_directory(self, input_dir: str, output_dir: Optional[str] = None, 
                          limit: Optional[int] = None) -> List[str]:
        """Process all JSON files in the input directory and add embeddings."""
        # Set up output directory
        if output_dir is None:
            output_dir = self.config["output_dir"]
        
        if not self.config.get("no_save", False):
            os.makedirs(output_dir, exist_ok=True)
        
        # Get all JSON files
        files = sorted(glob.glob(os.path.join(input_dir, "*.json")))
        if limit:
            files = files[:limit]
        
        logger.info(f"Found {len(files)} files to process for embeddings")
        
        # Process each file
        output_files = []
        
        for file_path in tqdm(files, desc="Generating embeddings"):
            try:
                # Generate embeddings
                doc_with_embeddings = self.generate_embeddings_for_file(file_path)
                
                if doc_with_embeddings:
                    if not self.config.get("no_save", False):
                        # Save to output directory
                        output_path = os.path.join(output_dir, os.path.basename(file_path))
                        self.save_document(doc_with_embeddings, output_path)
                        output_files.append(output_path)
                    else:
                        # If not saving, just track the document ID
                        output_files.append(doc_with_embeddings.get("document_id", "unknown"))
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
        
        logger.info(f"Successfully processed {len(output_files)} files for embeddings")
        return output_files


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate embeddings for PubMed chunks")
    parser.add_argument("--input", "-i", required=True, help="Directory containing processed JSON files")
    parser.add_argument("--output", "-o", default="./processed_with_embeddings", help="Output directory for files with embeddings")
    parser.add_argument("--limit", "-l", type=int, help="Limit the number of files to process")
    parser.add_argument("--model", "-m", default="pritamdeka/S-PubMedBERT-MS-MARCO", help="Sentence transformer model to use")
    parser.add_argument("--batch-size", "-b", type=int, default=32, help="Batch size for embedding generation")
    parser.add_argument("--no-save", action="store_true", help="Don't save intermediate files")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main():
    """Main function to run the embedding generator."""
    args = parse_args()
    
    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Create config from arguments
    config = {
        "model_name": args.model,
        "batch_size": args.batch_size,
        "output_dir": args.output,
        "no_save": args.no_save
    }
    
    # Create generator and process files
    generator = EmbeddingGenerator(config)
    processed_files = generator.process_directory(args.input, args.output, args.limit)
    
    logger.info(f"Generated embeddings for {len(processed_files)} files")
    return processed_files


if __name__ == "__main__":
    main()