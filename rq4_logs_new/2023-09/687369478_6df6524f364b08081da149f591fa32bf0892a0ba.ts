import express, { Request, Response } from "express";
const router = express.Router();

/* GET all questions. */
router.get('/', function(req: Request, res: Response) {
  res.json([{
    id: '456',
    name: 'bookName2',
    difficulty: 'easy'
  }])
});

/* GET question. */
router.get('/:id', function(req: Request, res: Response) {
  const id = req.params.id;
  res.json({
    id,
    name: 'bookName',
    difficulty: 'easy'
  })
});

/* POST question. */
router.post('/', function(req: Request, res: Response) {
  res.location('/123');
  res.end();
});

/* PUT question. */
router.put('/:id', function(req: Request, res: Response) {
  const id = req.params.id;
  res.location('/' + id);
  res.end();
});

/* DELETE question */
router.put('/:id', function(req: Request, res: Response) {
  res.end();
});

module.exports = router;