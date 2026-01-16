package persons;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Point;
import java.awt.Polygon;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferInt;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;

import javax.imageio.ImageIO;

public class Sprite {
	
	public ArrayList<BufferedImage> sprites = new ArrayList<BufferedImage>();
	public ArrayList<Shape> edges = new ArrayList<Shape>();
	public int ticks = 0;
	public int visibleSprite = 0;
	public ArrayList<Integer> timing = new ArrayList<Integer>();
	public int width;
	public int height;
	public int x;
	public int y;
	public double currentAngle = 0;
		
	public ArrayList<Point> offsets = new ArrayList<Point>();
	
	int iterator = 0;
//	public BufferedImage sprite;
//	private Polygon edges;
	private boolean polygon;
	ArrayList<Point> orderedList = new ArrayList<Point>();
	
	public Sprite(int startFrame, String... fileLocations) {
		try {
			for (String fileLocation : fileLocations) {
				sprites.add(ImageIO.read(new File(fileLocation)));
				timing.add(1);
			}
			
			System.out.println("Got image");
		} catch (IOException e) {
			System.err.println("File does not exist");
			e.printStackTrace();
		}
//		ifPolygon(sprite);
		for (int i = 0; i < fileLocations.length; i++) {
			if (fileLocations[i].indexOf(".png") == -1 || !ifPolygon(sprites.get(i))) {
				int[] xs = {0, sprites.get(i).getWidth(), 0, sprites.get(i).getWidth()};
				int[] ys = {0, 0, sprites.get(i).getHeight(), sprites.get(i).getHeight()};
	
				edges.add(new Polygon(xs, ys, xs.length));
				

			}
		}
	}
	
	public Sprite(String... fileLocations) {
		this(0, fileLocations);
	}
	
	public void setTiming(int... times) {
		for (int i = 0; i < times.length; i++) {
			timing.set(i, times[i]);
		}
	}
	
	public void setLocation(int x, int y, int w, int h) {
		this.x = x;
		this.y = y;
		width = w;
		height = h;
	}
	
	public void setAngle(double angle) {
		currentAngle = angle;
	}
	
	public void setPolygonOffset(int index, int deltaX, int deltaY) {
		offsets.get(index).setLocation(deltaX, deltaY);
	}
	
	public boolean ifPolygon(BufferedImage src) {
		iterator++;
//	    if (src.getType() != BufferedImage.TYPE_INT_ARGB)
//	        return false;

	    ArrayList<Point> borders = new ArrayList<Point>();
	    
	    int w = src.getWidth();
	    int h = src.getHeight();
	    width = w;
	    height = h;
	    x = 1;
	    y = 1;
	    ByteBuffer bb = convertImage(src, true);
	    
	    for (int r = 1; r < h - 1; r++) {
	    	for (int c = 1; c < w - 1; c++) {
		    	int sum = bSum(bb, r * w + c);
		    	if (sum != 0) {
//		    		System.out.println("Coord: (" + c + ", " + r + ")");
		    		
		    		int ula = bSum(bb, (r - 1) * w + c - 1);
	    			int uma = bSum(bb, (r - 1) * w + c);
	    			int ura = bSum(bb, (r - 1) * w + c + 1);
	    			int mla = bSum(bb, r * w + c - 1);
	    			int mra = bSum(bb, r * w + c + 1);
	    			int lla = bSum(bb, (r + 1) * w + c - 1);
	    			int lma = bSum(bb, (r + 1) * w + c);
	    			int lra = bSum(bb, (r + 1) * w + c + 1);
	    			int[] around = {ula, uma, ura, mla, mra, lla, lma, lra};
	    			int newSum = 0;
	    			for (int i : around) {
	    				newSum += (i != 0) ? 0 : 1;
	    			}
		    		if (newSum > 0 || r == 1 || r == h - 2 || c == 1 || c == w - 2) {

	    				borders.add(new Point(c, r));

	    				
	    			}
		    		
		    	}
	    	}
	    }
	    
	    
	    
	    orderedList.clear();
	    orderedList.add(borders.remove(0)); //Arbitrary starting point

	    while (borders.size() > 0) {
	       //Find the index of the closest point (using another method)
	       Point nearestIndex=findNearestIndex(orderedList.get(orderedList.size()-1), borders);
	       if (nearestIndex.y == -1) {
	    	   borders.remove(nearestIndex.x);
	       } else {
	    	 //Remove from the unorderedList and add to the ordered one
		       orderedList.add(borders.remove(nearestIndex.x));
	       }
	       
	    }
	    
	    
	   
	    
	    
		    
		    
//	    System.out.println("Polygon");
//	    System.out.println("X Coords: " + printArray(toArray(bordersX)));
//	    System.out.println("Y Coords: " + printArray(toArray(bordersY)));
	    offsets.add(new Point(0, 0));
	    edges.add(new Polygon(toArray(orderedList)[0], toArray(orderedList)[1], orderedList.size()));
	    return true;
	}
	
	public boolean ifPolygon(BufferedImage src, Graphics2D g) {
		boolean orig = ifPolygon(src); 
		for (int i = 0; i < iterator && i < orderedList.size(); i++) {
		    	g.drawRect(orderedList.get(i).x, orderedList.get(i).y, 1, 1);
			}
		return orig;
	}
	
	private Point findNearestIndex (Point thisPoint, ArrayList<Point> listToSearch) {
	    double nearestDistSquared=Double.POSITIVE_INFINITY;
	    int nearestIndex = listToSearch.indexOf(thisPoint) + 2;
	    for (int i=0; i< listToSearch.size(); i++) {
	    	Point point2=listToSearch.get(i);
	        double distsq = (thisPoint.x - point2.x)*(thisPoint.x - point2.x) 
	               + (thisPoint.y - point2.y)*(thisPoint.y - point2.y);
	        if(distsq < nearestDistSquared) {
	            nearestDistSquared = distsq;
	            nearestIndex=i;
	        }
	    }
	    System.out.println("Dist: " + nearestDistSquared);
	    return new Point(nearestIndex, (nearestDistSquared < 20) ? 1 : -1);
	}
	
	private int bSum(ByteBuffer bb, int i) {
		byte r = bb.get(i * 4);
    	byte g = bb.get(i * 4 + 1);
    	byte b = bb.get(i * 4 + 2);
    	byte a = bb.get(i * 4 + 3);
    	int sum = r + g + b + a;
//		System.out.println("rgba: " + r + ", " + g + ", " + b + ", " + a);

    	return sum;
	}
	
	 protected ByteBuffer convertImage(BufferedImage img, boolean invert)
	   {
	       ByteBuffer ret_val = null;
	       Raster raster = img.getRaster();
	       DataBuffer buffer = raster.getDataBuffer();
	       byte[] b_data;
	       int[] i_data;
	       byte[] tmp = new byte[4];
	       int height = img.getHeight(null);
	       int width = img.getWidth(null);

	       // OpenGL only like RGBA, not ARGB. Flip the order where necessary.
	       // Also, some cards don't like dealing with BGR textures, so
	       // automatically flip the bytes around the RGB for those that have it
	       // reversed.

	       switch(img.getType())
	       {
	           case BufferedImage.TYPE_4BYTE_ABGR:
	               b_data = ((DataBufferByte)buffer).getData();
	               ret_val = ByteBuffer.allocateDirect(b_data.length);
	               ret_val.order(ByteOrder.nativeOrder());

	               if(invert)
	               {
	                   int row_size = width * 4;
	                   int offset = b_data.length - row_size;

	                   for(int i = 0; i < height; i++)
	                   {
	                       for(int j = 0; j < width; j++)
	                       {
	                           tmp[0] = b_data[offset + j * 4 + 3];
	                           tmp[1] = b_data[offset + j * 4 + 2];
	                           tmp[2] = b_data[offset + j * 4 + 1];
	                           tmp[3] = b_data[offset + j * 4];

	                           ret_val.put(tmp, 0, 4);
	                       }

	                       offset -= row_size;
	                   }
	               }
	               else
	               {
	                   int row_size = width * 4;
	                   int offset = 0;

	                   for(int i = 0; i < height; i++)
	                   {
	                       for(int j = 0; j < width; j++)
	                       {
	                           tmp[0] = b_data[offset + j * 4 + 1];
	                           tmp[1] = b_data[offset + j * 4 + 2];
	                           tmp[2] = b_data[offset + j * 4 + 3];
	                           tmp[3] = b_data[offset + j * 4];

	                           ret_val.put(tmp, 0, 4);
	                       }

	                       offset += row_size;
	                   }
	               }
	               break;

	           case BufferedImage.TYPE_3BYTE_BGR:
	               b_data = ((DataBufferByte)buffer).getData();
	               ret_val = ByteBuffer.allocateDirect(b_data.length);
	               ret_val.order(ByteOrder.nativeOrder());

	               // Force the format change.
//	               format = FORMAT_RGB;

	               if(invert)
	               {
	                   int row_size = width * 3;
	                   int offset = b_data.length - row_size;

	                   for(int i = 0; i < height; i++)
	                   {
	                       for(int j = 0; j < width; j++)
	                       {
	                           tmp[0] = b_data[offset + j * 3 + 2];
	                           tmp[1] = b_data[offset + j * 3 + 1];
	                           tmp[2] = b_data[offset + j * 3];

	                           ret_val.put(tmp, 0, 3);
	                       }

	                       offset -= row_size;
	                   }
	               }
	               else
	                   ret_val.put(b_data, 0, b_data.length);
	               break;

	           case BufferedImage.TYPE_INT_RGB:
	               i_data = ((DataBufferInt)buffer).getData();

	               ret_val = ByteBuffer.allocateDirect(i_data.length * 4);
	               ret_val.order(ByteOrder.nativeOrder());

	               if(invert)
	               {
	                   IntBuffer buf = ret_val.asIntBuffer();
	                   int offset = i_data.length - width;

	                   for(int i = 0; i < height; i++)
	                   {
	                       buf.put(i_data, offset, width);
	                       offset -= width;
	                   }
	               }
	               else
	               {
	                   IntBuffer buf = ret_val.asIntBuffer();
	                   buf.put(i_data, 0, i_data.length);
	               }
	               break;

	           case BufferedImage.TYPE_INT_ARGB:
	               System.out.println("ARGB conversion not implemented yet");
	               break;

	           default:
	               System.out.println("Unsupported image type " + img.getType());
	       }

	       return ret_val;
	   }
	
	private int[][] toArray(ArrayList<Point> list) {
		int[][] nums = new int[2][list.size()];
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) != null)
				nums[0][i] = list.get(i).x;
				nums[1][i] = list.get(i).y;
		}
		return nums;
	}
	
	private String printArray(int[] arr) {
		String arrS = "[ ";
		for (int i : arr) {
			arrS += i + ", ";
			
		}
		arrS = arrS.substring(0, arrS.length() - 2);
		arrS += "]";
		return arrS;
	}
	
	public void drawSprite(Graphics2D g) {
		g.setColor(Color.WHITE);
//		for (int i = 0; i < sprites.size(); i++) {
//			g.drawImage(sprites.get(i), 1, 1, null);
//			g.draw(edges.get(i));
//		}
//		System.out.println("Ticks: " + ticks);
//		System.out.println("Visible Sprite: " + visibleSprite);
//		System.out.println("Timing: " + timing.get(visibleSprite));

		
		Image image;
		if (width >= height) {
			image = sprites.get(visibleSprite).getScaledInstance(width, -1, Image.SCALE_DEFAULT);
		} else {
			image = sprites.get(visibleSprite).getScaledInstance(-1, height, Image.SCALE_DEFAULT);
		}
		double scale = (double) image.getWidth(null) / sprites.get(visibleSprite).getWidth();
		
//		g.drawImage(image, x + (int) Math.ceil(offsets.get(visibleSprite).x * scale), y + (int) Math.ceil(offsets.get(visibleSprite).y * scale), null);

		AffineTransform transform = new AffineTransform();
		transform.setToRotation(currentAngle, image.getWidth(null) / 2 + x, image.getHeight(null) / 2 + y);

		System.out.println("Current Angle: " + currentAngle);
		Shape transShape = transform.createTransformedShape(edges.get(visibleSprite));
		transform.translate(x + (int) Math.ceil(offsets.get(visibleSprite).x * scale), y + (int) Math.ceil(offsets.get(visibleSprite).y * scale));
		g.drawImage(image, transform, null);
		g.draw(transShape);

	}
	
	public void updateSprite() {
		ticks++;
		if (ticks > timing.get(visibleSprite)) {
			ticks = 0;
			visibleSprite++;
			if (visibleSprite >= sprites.size()) visibleSprite = 0;
		}
//		System.out.println("Translate x: " + (edges.get(visibleSprite).getBounds().x - x));
//		System.out.println("Translate y: " + (edges.get(visibleSprite).getBounds().y - y));

		Image image;
		if (width >= height) {
			image = sprites.get(visibleSprite).getScaledInstance(width, -1, Image.SCALE_DEFAULT);
		} else {
			image = sprites.get(visibleSprite).getScaledInstance(-1, height, Image.SCALE_DEFAULT);
		}
		double scale = (double) image.getWidth(null) / edges.get(visibleSprite).getBounds().width;
		
		AffineTransform transform = new AffineTransform();
		
		transform.scale(scale, scale);
		edges.set(visibleSprite, transform.createTransformedShape(edges.get(visibleSprite)));
		scale = (double) edges.get(visibleSprite).getBounds().width / sprites.get(visibleSprite).getWidth(null);

		int polygonX = x - edges.get(visibleSprite).getBounds().x + (int) Math.ceil(offsets.get(visibleSprite).x * scale);
		int polygonY = y - edges.get(visibleSprite).getBounds().y + (int) Math.ceil(offsets.get(visibleSprite).y * scale);
	
		transform.setToTranslation(polygonX, polygonY);
		edges.set(visibleSprite, transform.createTransformedShape(edges.get(visibleSprite)));
		

	}
}