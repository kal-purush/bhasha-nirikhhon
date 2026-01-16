from sgmllib import SGMLParser
import htmlentitydefs, time

class Annotation(SGMLParser):
	def __init__(self, chain, replacement):
		self.pieces = []
		self.chain = chain
		self.replacement = replacement
		self.annotate = False
		SGMLParser.reset(self)

	def unknown_starttag(self, tag, attrs):
		if tag=="head" or tag=="footer":
			self.annotate = False
			
		strattrs = "".join([' %s="%s"' % (key, value) for key, value in attrs])
		self.pieces.append("<%(tag)s%(strattrs)s>" % locals())
		
	def unknown_endtag(self, tag):
		if tag=="head" or tag=="footer":
			self.annotate = True
			
		self.pieces.append("</%(tag)s>" % locals())

	def handle_charref(self, ref):
		self.pieces.append("&#%(ref)s;" % locals())
		
	def handle_entityref(self, ref):
		self.pieces.append("&%(ref)s" % locals())
		
		if htmlentitydefs.entitydefs.has_key(ref):
			self.pieces.append(";")

	def handle_data(self, text):
		if self.annotate:
			#print time.ctime() +": Annotation::handle_data ==> Replacing <" + self.chain +"> by <"+self.replacement+">"
			temp = text.replace(self.chain, self.replacement)
		else:
			temp = text
			
		self.pieces.append(temp)
		
	def handle_comment(self, text):
		self.pieces.append("<!--%(text)s-->" % locals())
		
	def handle_pi(self, text):
		self.pieces.append("<?%(text)s>" % locals())

	def handle_decl(self, text):
		self.pieces.append("<!%(text)s>" % locals())
		
	def output(self):
		return "".join(self.pieces)