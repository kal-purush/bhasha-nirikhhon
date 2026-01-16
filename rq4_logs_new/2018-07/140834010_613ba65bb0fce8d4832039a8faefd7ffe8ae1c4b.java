package jhi.barcodr.gui;

import com.google.zxing.*;

import org.eclipse.jface.viewers.*;
import org.eclipse.swt.widgets.*;

import jhi.swtcommons.gui.viewer.*;

/**
 * @author Sebastian Raubach
 */
public class BarcodeComboViewer extends AdvancedComboViewer<BarcodeFormat>
{
	private BarcodeFormat[] ACCEPTED_FORMATS = {BarcodeFormat.CODE_128, BarcodeFormat.CODE_39, BarcodeFormat.EAN_13, BarcodeFormat.EAN_8, BarcodeFormat.UPC_A, BarcodeFormat.QR_CODE, BarcodeFormat.RSS_EXPANDED};

	public BarcodeComboViewer(Composite composite, int style)
	{
		super(composite, style);

		setInput(ACCEPTED_FORMATS);
		setLabelProvider(new LabelProvider()
		{
			@Override
			public String getText(Object barcodeFormat)
			{
				if (barcodeFormat instanceof BarcodeFormat)
				{
					if (barcodeFormat == BarcodeFormat.RSS_EXPANDED)
						return "All";
					else
						return ((BarcodeFormat) barcodeFormat).name();
				}
				return "";
			}
		});
		setSelection(new StructuredSelection(BarcodeFormat.CODE_128));
	}

	@Override
	protected String getDisplayText(BarcodeFormat barcodeFormat)
	{
		return barcodeFormat.name();
	}
}