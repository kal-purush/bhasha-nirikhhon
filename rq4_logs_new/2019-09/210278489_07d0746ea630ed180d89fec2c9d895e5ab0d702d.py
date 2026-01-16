class scGeneralAnalize:

    def __init__(self, data, min_counts=None, min_genes=200, min_cells=3, min_n_genes=2500, min_percent_mito=0.05,
                 TPM=False, gene_len=None, libmethod=None,
                tSNE=False, UMAP=False,n_components=2, n_comps = 50, log_transform=True):
        self.data = data 
        self.data = ann.AnnData(self.data)
        self.min_counts = min_counts
        self.min_genes = min_genes
        self.min_cells = min_cells
        self.min_n_genes = min_n_genes
        self.min_percent_mito = min_percent_mito
        
        self.gene_len = gene_len
        self.TPM = TPM
        self.libmethod = libmethod
        
        self.tSNE = tSNE
        self.UMAP = UMAP
        self.n_components = n_components
        self.n_comps = n_comps
        self.results = pd.DataFrame()
        
        
    def Mix(self):
        
        def Preprocessing(inplace=True):

            sc.pp.filter_cells(self.data, self.min_counts, self.min_genes,inplace=True)
            sc.pp.filter_genes(self.data,self.min_counts, self.min_cells,inplace=True)

            mito_genes = self.data.var_names.str.startswith('MT-')
            self.data.obs['percent_mito'] = np.sum(self.data[:, mito_genes].X, axis=1) / np.sum(self.data.X, axis=1)
            self.data.obs['n_counts'] = self.data.X.sum(axis=1)

            self.data = self.data[self.data.obs['n_genes'] < self.min_n_genes, :]
            self.data = self.data[self.data.obs['percent_mito'] < self.min_percent_mito, :]
            
            print("Preprocessing finished")

            return self.data
        
        def normalizing():

            if self.TPM:
                def toTPM():
                    raw = self.data
                    self.libmethod = libmethod 
                    libmethod_keywords = ['10x chrominum', 'drop-seq', 'microwell-seq', 'C1 Fluidigm', 'inDrops', 'Smart-seq2',
                                            'CEL-seq', 'ATAC-seq']
                    full_len_keywords = ['C1 Fluidigm', 'Smart-seq2']

                    if self.libmethod in libmethod_keywords:
                        if self.libmethod in full_len_keywords:
                            self.gene_len = True
                        else:
                            self.gene_len = False
                    cellid = raw.obs.index.tolist()
                    ref_dict = {}
                    raw = raw[:, 1:] 
                    names = raw.var.index.tolist()
                    names = [x for x in names if not x.startswith('ERCC', 0)]
                    raw = raw[:, names]
                    ###print(self.data)
                    if self.gene_len:
                        df = self.gene_ref()
                        if names[0].startswith('ENS', 0):
                            for i, x in enumerate(df['Gene_ID'].tolist()):
                                ref_dict[x] = df['Gene_length'].tolist()[i]
                        else:
                            for i, x in enumerate(df['Gene_name'].tolist()):
                                ref_dict[x] = df['Gene_length'].tolist()[i]

                        leng = []
                        for i, name in enumerate(names):
                            try:
                                leng.append(int(ref_dict[name]))
                            except:
                                leng.append(0)

                        leng = np.array(leng)
                        import statistics as st
                        leng[np.where(leng == 0)] = st.median(np.array(leng[leng > 0]))
                        raw = raw.div(leng, axis=1)
                    rsum = raw.X.sum(axis=1)
                    tpm = pd.DataFrame(raw.X).div(rsum, axis=0) * 1e6  
                    tpm = tpm.fillna(0)
                    tpm.insert(0, "normalizationMethod", "TPM from raw data")
                    tpm.insert(1, "cellID", cellid)

                    self.data = tpm
                    ####print(self.data)
                    return self.data
                    toTPM()
            else:
                sc.pp.normalize_per_cell(self.data, counts_per_cell_after=1e4)
                sc.pp.log1p(self.data)  
            
            print("normalizing finished")

            return self.data
        
        def calculate_dim_red():
            self.embedding_train = None
            sc.pp.highly_variable_genes(self.data, n_top_genes=500)
            sc.pp.pca(self.data, n_comps=self.n_comps, zero_center=True)
            X_pca = self.data.obsm['X_pca']
            tSNE_init = X_pca[:, :2]
            print('feature selection and PCA compression finished ')

            if self.UMAP:
                import umap
                reducer = umap.UMAP(n_components=n_components)
                X_embedded = reducer.fit_transform(X_pca)
                self.results['UMAP1'] = X_embedded[:, 0].tolist()
                if n_components == 2:
                    self.results['UMAP2'] = X_embedded[:, 1].tolist()
                print('UMAP finished')

            if self.tSNE:
                from openTSNE import TSNE
                from openTSNE.callbacks import ErrorLogger

                tsne = TSNE(perplexity=30,
                callbacks=ErrorLogger(),
                initialization='pca',
                random_state=42,
                early_exaggeration_iter=50,
                n_components=2)

                %time embedding_train = tsne.fit(X_pca)
                self.embedding_train = embedding_train
                

                self.results['tSNE1'] = embedding_train.T[0].tolist()
                self.results['tSNE2'] = embedding_train.T[1].tolist()
                print('tSNE finished')
            return self.data, self.results
        
        
        
        
        Preprocessing()
        normalizing()
        calculate_dim_red()
        self.data = self.data.X
        self.results = self.results.to_numpy
        return self.data,self.results