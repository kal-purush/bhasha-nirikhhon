package com.evolveum.midpoint.client.impl.restjaxb;

import com.evolveum.midpoint.client.api.ObjectGenerateService;
import com.evolveum.midpoint.client.api.ObjectModifyService;
import com.evolveum.midpoint.client.api.TaskFuture;
import com.evolveum.midpoint.client.api.exception.AuthenticationException;
import com.evolveum.midpoint.client.api.exception.AuthorizationException;
import com.evolveum.midpoint.client.api.exception.ObjectNotFoundException;
import com.evolveum.midpoint.xml.ns._public.common.common_3.ObjectType;
import com.evolveum.prism.xml.ns._public.types_3.ModificationTypeType;
import com.sun.org.apache.xpath.internal.functions.FuncSubstring;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jakmor
 */
public class RestJaxbObjectGenerateService<O extends ObjectType> extends AbstractObjectWebResource<O> implements ObjectGenerateService<O>
{

    private static final String DEFAULT_PASS_POLICY_OID ="00000000-0000-0000-0000-000000000003";
    private static final String DEFAULT_PASS_PATH = "/credentials/password/value";

    private boolean execute = false;
    private String path = DEFAULT_PASS_PATH ;
    private String policyOid = DEFAULT_PASS_POLICY_OID;

    public RestJaxbObjectGenerateService(RestJaxbService service, Class<O> type, String oid)
    {
        super(service, type, oid);
    }

    @Override
    public ObjectGenerateService<O> execute(){
        this.execute = true;
        return this;
    }

    @Override
    public ObjectGenerateService<O> path(String path){
        this.path = path;
        return this;
    }

    @Override
    public ObjectGenerateService<O> policy(String policyOid){
        this.policyOid = policyOid;
        return this;
    }


    @Override
    public TaskFuture apost() throws AuthorizationException, ObjectNotFoundException, AuthenticationException
    {
        String oid = getOid();
        String restPath = RestUtil.subUrl(Types.findType(getType()).getRestPath(), oid);

        Response response = getService().getClient().replacePath(restPath).post("");

        switch (response.getStatus()) {
            case 204:
                RestJaxbObjectReference<O> ref = new RestJaxbObjectReference<>(getService(), getType(), oid);
                return new RestJaxbCompletedFuture<>(ref);
            case 400:
                throw new BadRequestException(response.getStatusInfo().getReasonPhrase());
            case 401:
                throw new AuthenticationException(response.getStatusInfo().getReasonPhrase());
            case 403:
                throw new AuthorizationException(response.getStatusInfo().getReasonPhrase());
                //TODO: Do we want to return a reference? Might be useful.
            case 404:
                throw new ObjectNotFoundException(response.getStatusInfo().getReasonPhrase());
            default:
                throw new UnsupportedOperationException("Implement other status codes, unsupported return status: " + response.getStatus());
        }
    }

    private buildPolicyItemsDefinition(){

    }
}