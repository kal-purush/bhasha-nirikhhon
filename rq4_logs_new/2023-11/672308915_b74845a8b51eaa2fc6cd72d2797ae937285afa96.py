import numpy as np
from sentence_transformers import SentenceTransformer


# follows the services.SourceTasks interface
class TaskCatalog:
    provider_name = "nlp"

    @staticmethod
    def embedding(text: str):
        model = SentenceTransformer("all-MiniLM-L6-v2")
        sentences = text.split("\n")

        # Sentences are encoded by calling model.encode()
        embeddings = model.encode(sentences)

        # Print the embeddings
        embs = []
        for sentence, embedding in zip(sentences, embeddings):
            print("Sentence:", sentence)
            print("Embedding:", embedding)
            print("")
            embs.append(embedding)

        embedding = np.mean(embs)

        return {"embedding": embedding}