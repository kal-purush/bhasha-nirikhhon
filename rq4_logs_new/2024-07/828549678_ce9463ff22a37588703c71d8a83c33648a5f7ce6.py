from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import uuid
import together
import subprocess
import tempfile
import torch
from dotenv import load_dotenv
from transformers import (
  AutoTokenizer,
  EsmForProteinFolding,
  set_seed
)

load_dotenv()  # Load environment variables from .env file

app = FastAPI()
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")

class ModelInput(BaseModel):
    prompt: str = (
        "|" # -> start prompt
        +"d__Bacteria;"
        +"p__Pseudomonadota;"
        +"c__Gammaproteobacteria;"
        +"o__Enterobacterales;"
        +"f__Enterobacteriaceae;"
        +"g__Escherichia;"
        +"s__Escherichia"
        +"|" # -> end prompt
    )
    max_tokens: int = 1024
    temperature: float = 1.0
    top_k: int = 4
    top_p: float = 1.0
    logprobs: bool = False

class ProteinInput(BaseModel):
    dna_sequence: str = "GCCGAGCCCCAGCCCAGGTACACGAAGCACCCGGAGCGCGCGAGCGCCCGATCTAGCATCCGCCCCCCCCCCCCCCCTCTCATCTGCCGCGGGCTGCGCACGACCGGCATCCGGCGCCCGCGTAGCACCCCCCCCCCCCCAATGCCACGCGCTGGGCGCTCATGGCTATCTCCATCCCACGCCCATTCGTTTGTTCGCCAAAGGGGxCGCGCGTAGCCCGCCCGGGGGCCGCCCGCCTTCGTCGCCGCCACTCGCCCGGGGCGGCCCGCCGCTCGGGGCGGCCGGGCGCCGGCTGTTCCCTCAGCGCCCTCCGCCGCTCCCAGGGGGGGTCCCGCGTGCGCCGCCCTGCTTTTCCGGCGCGCGGCGCGGCCGCAGTCAACGCGCCGCTCCTCGCACATGGTCATCCACCCGCGCGCATCTGAGCCGCCACAGCGGCCCAAGGCCACCCTCGTCTCGCGCTTGCCTCCCGGGGAGTCCCGGCTCTGCCTCCGTCATCTCCCCTGCCCCCCCGCACCGCCCCCCCCCCCCAGGCACCCCCGGGCCCCAACATACGCCGTCCGGCTGTACCTTTCTGCCTCGCTTATGCGGTAGCCTTTGTATCCGTCGACACTAGGGCGCCGGCCTGTCGCACGGTGCACCAGATGCTCCCGCACGGTTTTCGCCGCCTTTTCTTCCACCCGTACCCCCCCCCCCCCCTAGCCCGCCCCGCCTCCCGCTTGCCCGCCCGCCCCGTCGCCCGCAGTTGGGGTGCCCTCTTACCGGGGCGTGGCTGCGCGCCCGATTTGCCCCTGGCTCCCCACGCGCGGTCACTATCCCCCTGCGTTCAACGCGGGGCTCCCCCCCCATACTCCTTCCACCCCCTCCGGCAAGGAGCGTCGCGCGCGCCCGGCAGCTCTTTACCCGCATCTTTCCGCGCGCGGCAGCAGCCGCCTCCACCCAGGGGGCGCCCTTTCCTCCCGCGTTCTGCATTGCTGGTCCAGGGCGGCCCTTTCCGCCAGTTGGGGCGCCCGTTTCTTTTCTGCCC"

class ProteinOutput(BaseModel):
    protein_sequences: str

@app.post("/generate_dna")
async def generate_dna(input: ModelInput):
    # Code to generate DNA using Evo
    # Dummy DNA sequence for example
    together.api_key = TOGETHER_API_KEY

    output = together.Complete.create(
        prompt = input.prompt,
        model = 'togethercomputer/evo-1-131k-base',
        max_tokens = input.max_tokens,
        temperature = input.temperature,
        top_k = input.top_k,
        top_p = input.top_p,
        logprobs = input.logprobs
    )
    gen_dna_seq = output['choices'][0]['text']
    return {"dna_sequence": gen_dna_seq}

@app.post("/predict_genes")
async def predict_genes(input: ProteinInput):
    unique_id = uuid.uuid4().hex
    temp_fasta_path = f"temp_{unique_id}.faa"
    proteins_faa_path = f"proteins_{unique_id}.faa"
    genes_gff_path = f"genes_{unique_id}.gff"

    with open(temp_fasta_path, "w") as temp_fasta:
        temp_fasta.write(f">sequence\n{input.dna_sequence}")
    
    result = subprocess.run(
        ["prodigal", "-i", temp_fasta_path, "-a", proteins_faa_path, "-o", genes_gff_path, "-p", "meta", "-f", "gff"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise HTTPException(status_code=400, detail="Prodigal failed")
    
    with open(proteins_faa_path) as f:
        protein_sequences = f.read()
    
    os.remove(temp_fasta_path)
    os.remove(proteins_faa_path)
    os.remove(genes_gff_path)
    
    return {"protein_sequences": protein_sequences}

@app.post("/predict_protein_structure")
async def predict_protein_structure(input: ProteinOutput):
    esmfold = EsmForProteinFolding.from_pretrained(
        "facebook/esmfold_v1",
        low_cpu_mem_usage = True # we set this flag to save some RAM during loading
    )
    model = esmfold.ESMFold()
    model.eval()
    with torch.no_grad():
        output = model(input.protein_sequences)
    # Save the output PDB file and return its path
    pdb_file = "protein_structure.pdb"
    with open(pdb_file, "w") as f:
        f.write(output["pdb_string"])
    return {"pdb_file": pdb_file}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)