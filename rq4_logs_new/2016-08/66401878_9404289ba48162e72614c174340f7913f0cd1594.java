package model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;
import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.context.FacesContext;
import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.StreamedContent;
import org.primefaces.model.UploadedFile;

@ManagedBean
@javax.faces.view.ViewScoped
public class FileUploadView {

	private UploadedFile file;
	private StreamedContent fotoSalva;
	private String endereco = "/home/geovane/geovane.jpg";

	public UploadedFile getFile() {
		return file;
	}

	public void setFile(UploadedFile file) {
		this.file = file;
	}

	public StreamedContent getFotoSalva() {
		return fotoSalva;
	}

	public void setFotoSalva(StreamedContent fotoSalva) {
		this.fotoSalva = fotoSalva;
	}

	public String getEndereco() {
		return endereco;
	}

	public void setEndereco(String endereco) {
		this.endereco = endereco;
	}

	public void upload() {
		if (file != null) {
			// String pasta = System.getenv("OPENSHIFT_DATA_DIR");
			String pasta = "/home/geovane/";
			String nmArquivo = (Calendar.getInstance().getTimeInMillis() / 1000)
					+ file.getFileName();
			File file1 = new File(pasta, nmArquivo);
			try {
				FileOutputStream fos = new FileOutputStream(file1);
				fos.write(file.getContents());
				fos.close();
				this.endereco = pasta + nmArquivo;
				FacesMessage message = new FacesMessage("Succesful",
						this.endereco + " is uploaded.");
				FacesContext.getCurrentInstance().addMessage(null, message);
			} catch (FileNotFoundException e) {
				System.out.println(e.getMessage());
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	@PostConstruct
	public void init() {
		try {
			System.out.println(this.endereco);
			File f = new File(this.endereco);
			this.fotoSalva = new DefaultStreamedContent(new FileInputStream(f));
			System.out.println("Foi executado!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}