package una.android.rapimoncha.activities.adapter;

import java.util.ArrayList;

import org.w3c.dom.Text;

import una.android.rapimoncha.R;
import una.android.rapimoncha.entidades.Galeria;
import una.android.rapimoncha.entidades.Producto;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.util.Base64;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;

public class ProductosComercioAdapter extends ArrayAdapter<Producto> {
	Context context;
	ArrayList<Producto> productos;

	public ProductosComercioAdapter(Context context,
			ArrayList<Producto> productos) {

		super(context, R.layout.layout_producto_listviewitem, productos);
		this.context = context;
		this.productos = productos;
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getCount() {
		// TODO Auto-generated method stub
		return productos.size();

	}

	public void setProductos(ArrayList<Producto> productos) {
		this.productos = productos;
	}

	@Override
	public View getView(int position, View converView, ViewGroup parent) {
		View v = converView;
		if (v == null) {
			v = ((LayoutInflater) context
					.getSystemService(Context.LAYOUT_INFLATER_SERVICE))
					.inflate(R.layout.layout_producto_listviewitem, parent,
							false);
		}

		Producto p = productos.get(position);
		// parte de la imagen
		try {
			ImageView img = (ImageView) v.findViewById(R.id.lproduimgprodu);
			if (p != null && p.getImagenes() != null
					&& p.getImagenes().size() > 0) {
				Galeria g = p.getImagenes().get(0);

				
				byte[] decodedString = Base64.decode(g.getDtImagen(),
						Base64.DEFAULT);
				Bitmap decodedByte = BitmapFactory.decodeByteArray(
						decodedString, 0, decodedString.length);
				img.setImageBitmap(decodedByte);
			}else{
				img.setImageDrawable(context.getResources().getDrawable(R.drawable.icon_productitem));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// resto de datos
		TextView lblnoproducto=(TextView)v.findViewById(R.id.lprodulblnoproduct);
		lblnoproducto.setText(p.getNoProducto());
		
		TextView lbldeproducto=(TextView)v.findViewById(R.id.lprodudeproduc);
		lbldeproducto.setText(p.getDeProducto());
		
		TextView lblpeproducto=(TextView)v.findViewById(R.id.lprodupeprodu);
		lblpeproducto.setText(p.getPrProducto()+"");
		return v;
	}
}