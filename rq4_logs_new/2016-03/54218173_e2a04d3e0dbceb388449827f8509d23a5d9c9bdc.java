package flint.data.aggregate;

// Core Java classes
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;


/**
 *
 * @author Philip Bowditch
 */
public class HashAggregator extends AbstractAggregator {

    protected MessageDigest m_messageDigest;
    
    public HashAggregator() {
        super();
        
        m_messageDigest = MessageDigest.getInstance( "MD5" );
        m_name          = "MD5";
    }
    
    @Override
    public void reset() {
        m_messageDigest.reset();
    }
    
    @Override
    public void aggregate( String value ) {
        
        if ( m_amount == null ) {
            return;
        }
        
        String newValue = value;
        if ( newValue == null ) {
            if ( m_ignoreNull ) {
                newValue = "";
            }
            else {
                m_amount = null;
                return;
            }
        }
        
        
        try {
            byte[] bytesOfMessage = newValue.getBytes("UTF-8");
            
            m_messageDigest.update( bytesOfMessage );
        }
        catch ( Exception ex ) {}
        
    }
    
    @Override
    public String getResult() {
        byte[] bytes = m_messageDigest.digest();
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    @Override
    public void setResult( String result ) {
        // throw new NotImplementedException();
        //m_amount = result;
    }
    
}