import java.util.PriorityQueue;

/**
 * Interface that all compression suites must implement. That is they must be
 * able to compress a file and also reverse/decompress that process.
 * 
 * @author Brian Lavallee
 * @since 5 November 2015
 * @author Owen Atrachan
 * @since December 1, 2016
 */
public class HuffProcessor {

	public static final int BITS_PER_WORD = 8;
	public static final int BITS_PER_INT = 32;
	public static final int ALPH_SIZE = (1 << BITS_PER_WORD); // or 256
	public static final int PSEUDO_EOF = ALPH_SIZE;
	public static final int HUFF_NUMBER = 0xface8200;
	public static final int HUFF_TREE = HUFF_NUMBER | 1;
	public static final int HUFF_COUNTS = HUFF_NUMBER | 2;

	public enum Header {
		TREE_HEADER, COUNT_HEADER
	};

	public Header myHeader = Header.TREE_HEADER;

	/**
	 * Compresses a file. Process must be reversible and loss-less.
	 *
	 * @param in
	 *            Buffered bit stream of the file to be compressed.
	 * @param out
	 *            Buffered bit stream writing to the output file.
	 */
	public void compress(BitInputStream in, BitOutputStream out) {
		int[] counts = readForCounts(in);
		TreeNode root = makeTreeFromCounts(counts);
		String[] codings = makeCodingsFromTree(root);
		writeHeader(root, out);
		in.reset();
		writeCompressedBits(in, codings, out);
	}

	private void writeCompressedBits(BitInputStream in, String[] codings, BitOutputStream out) {
		
	}

	private void writeHeader(TreeNode root, BitOutputStream out) {
		out.writeBits(32, HUFF_NUMBER);
		visit(root, out);
	}

	private void visit(TreeNode root, BitOutputStream out) {
		if (root != null) {
			if (root.myLeft == null && root.myRight == null) {
				out.writeBits(1, 1);
				out.writeBits(9, root.myValue);
			}
			out.writeBits(1, 0);
			visit(root.myLeft, out);
			visit(root.myRight, out);
		}
	}

	private String[] makeCodingsFromTree(TreeNode root) {
		String[] ret = new String[257];
		helperMethod(root, "", ret);
		return ret;
	}

	public void helperMethod(TreeNode root, String num, String[] b) {
		if (root == null)
			return;
		if (root.myLeft == null && root.myRight == null) {
			b[root.myValue] = num;
			return;
		}
		helperMethod(root.myLeft, num + "0", b);
		helperMethod(root.myRight, num + "1", b);
	}

	private TreeNode makeTreeFromCounts(int[] counts) {
		PriorityQueue<TreeNode> pq = new PriorityQueue<>();
		for (int k = 0; k < counts.length; k++) {
			pq.add(new TreeNode(k, counts[k], null, null));
		}
		pq.add(new TreeNode(PSEUDO_EOF, 0));
		while (pq.size() > 1) {
			TreeNode left = pq.remove();
			TreeNode right = pq.remove();
			pq.add(new TreeNode(0, left.myWeight + right.myWeight, left, right));
		}
		TreeNode root = pq.remove();
		return root;
	}

	private int[] readForCounts(BitInputStream in) {
		int[] ret = new int[256];
		int num = in.readBits(8);
		while (num != -1) {
			ret[num]++;
			num = in.readBits(8);
		}
		return ret;
	}

	/**
	 * Decompresses a file. Output file must be identical bit-by-bit to the
	 * original.
	 *
	 * @param in
	 *            Buffered bit stream of the file to be decompressed.
	 * @param out
	 *            Buffered bit stream writing to the output file.
	 */
	public void decompress(BitInputStream in, BitOutputStream out) {
		int id = in.readBits(BITS_PER_INT);
		if (id != HUFF_NUMBER && id != HUFF_TREE) {
			throw new HuffException("bad input, no PSEUDO_EOF");
		}
		TreeNode root = readTreeHeader(in);
		readCompressedBits(root, in, out);
	}

	private void readCompressedBits(TreeNode root, BitInputStream in, BitOutputStream out) {
		TreeNode current = root;
		int bit = in.readBits(1);
		while (bit != -1) {
			if (bit == 0) {
				current = current.myLeft;
			} else {
				current = current.myRight;
			}
			if (current.myLeft == null && current.myRight == null) {
				if (current.myValue == PSEUDO_EOF)
					break;
				else {
					out.writeBits(8, current.myValue); 
					current = root;
				}
			}
			bit = in.readBits(1);
		}
	}

	private TreeNode readTreeHeader(BitInputStream in) {
		TreeNode t = new TreeNode(-1, -1);
		int flag = in.readBits(1);
		if (flag == 0) {
			t.myLeft = readTreeHeader(in);
			t.myRight = readTreeHeader(in);
			return t;
		}
		t.myValue = in.readBits(9);
		return t;
	}

	public void setHeader(Header header) {
		myHeader = header;
		System.out.println("header set to " + myHeader);
	}
}