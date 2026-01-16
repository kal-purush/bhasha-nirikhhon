using Google.Cloud.Firestore;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using documents_ms.models;

namespace documents_ms.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DocumentsController : ControllerBase
    {

        private readonly FirestoreDb _firestore;

        public DocumentsController(FirestoreDb firestore)
        {
            _firestore = firestore;
        }

        // GET: api/Documents
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Document>>> GetDocuments()
        {
            var snapshot = await _firestore.Collection("Documents").GetSnapshotAsync();
            var documents = snapshot.Documents.Select(doc => {
                var document = doc.ConvertTo<Document>();
                document.Id = doc.Id;
                return document;
            }).ToList();
            return documents;
        }

        // GET: api/Documents/{id}
        [HttpGet("{id}")]
        public async Task<ActionResult<Document>> GetDocument(string id)
        {
            var docRef = _firestore.Collection("Documents").Document(id);
            var snapshot = await docRef.GetSnapshotAsync();

            if (!snapshot.Exists)
                return NotFound();

            var document = snapshot.ConvertTo<Document>();
            document.Id = docRef.Id;
            return document;
        }

        // POST: api/Documents
        [HttpPost]
        public async Task<ActionResult<Document>> CreateDocument(Document doc)
        {
            doc.DateCreated = DateTime.UtcNow;
            var docRef = await _firestore.Collection("Documents").AddAsync(doc);
            doc.Id = docRef.Id;

            return CreatedAtAction(nameof(GetDocument), new { id = doc.Id }, doc);
        }

        // PUT: api/Documents/{id}
        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateDocument(string id, Document doc)
        {
            var docRef = _firestore.Collection("Documents").Document(id);
            await docRef.SetAsync(doc, SetOptions.MergeAll);

            return NoContent();
        }

        // DELETE: api/Documents/{id}
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteDocument(string id)
        {
            var docRef = _firestore.Collection("Documents").Document(id);
            await docRef.DeleteAsync();

            return NoContent();
        }
    }
}