package fi.vm.sade.eperusteet.utils.audit;

import fi.vm.sade.auditlog.Changes;
import fi.vm.sade.eperusteet.utils.revision.RevisionMetaService;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static java.util.Arrays.asList;

@Slf4j
@Aspect
@Component
public class AuditLogger {

    @Autowired(required = false)
    private RevisionMetaService revisionMetaService;

    @Autowired(required = false)
    private CommonAuditer audit;

    @Around("execution(* *(..)) && within(@org.springframework.web.bind.annotation.RestController *))")
    public Object auditLog(ProceedingJoinPoint point) throws Throwable {

        Signature signature = point.getStaticPart().getSignature();
        if (signature instanceof MethodSignature) {
            MethodSignature method = (MethodSignature) signature;
            RequestMapping mapping = method.getMethod().getAnnotation(RequestMapping.class);

            if (mapping != null && mapping.method() != null && mapping.method().length > 0 && !asList(mapping.method()).contains(RequestMethod.GET)) {
                String[] parameterNames = method.getParameterNames();
                Object[] parameters = point.getArgs();
                Annotation[][] parameterAnnotations = method.getMethod().getParameterAnnotations();
                String name = method.getName();
                String controllerName = method.getMethod().getDeclaringClass().getName();
                Map<String, Object> params = new HashMap<>();
                for (int idx = 0; idx < parameters.length; ++idx) {
                    if (hasPathVariable(parameterAnnotations[idx])) {
                        params.put(parameterNames[idx], parameters[idx]);
                    }
                }
                Number currentRevision = revisionMetaService.getCurrentRevision();
                Changes.Builder changes = new Changes.Builder();

                try {
                    return point.proceed();
                }
                catch (RuntimeException ex) {
                    changes.added("failed", "true");
                    throw ex;
                }
                finally {
                    changes.updated("rev", currentRevision.toString(), revisionMetaService.getCurrentRevision().toString());
                    audit.log(name, controllerName, changes.build(), params);
                }
            }
        }
        return point.proceed();
    }

    private boolean hasPathVariable(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation instanceof PathVariable) {
                return true;
            }
        }
        return false;
    }

}