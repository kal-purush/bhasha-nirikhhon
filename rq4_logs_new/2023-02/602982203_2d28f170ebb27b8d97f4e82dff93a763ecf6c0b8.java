package com.joizhang.naiverpc.spring.beans.factory.annotation;

import com.joizhang.naiverpc.annotation.NaiveRpcService;
import com.joizhang.naiverpc.spring.context.annotation.NaiveRpcClassPathBeanDefinitionScanner;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.lang.annotation.Annotation;
import java.util.*;

@Slf4j
public class ServiceAnnotationPostProcessor implements BeanDefinitionRegistryPostProcessor, EnvironmentAware,
        ResourceLoaderAware, BeanClassLoaderAware, ApplicationContextAware, InitializingBean {

    public static final String BEAN_NAME = "naiveRpcServiceAnnotationPostProcessor";

    private static final List<Class<? extends Annotation>> serviceAnnotationTypes =
            Collections.singletonList(NaiveRpcService.class);

    protected final Set<String> packagesToScan;

    private Set<String> resolvedPackagesToScan;

    private Environment environment;

    private ResourceLoader resourceLoader;

    private ClassLoader classLoader;

    private BeanDefinitionRegistry registry;

    private ServicePackagesHolder servicePackagesHolder;

    private volatile boolean scanned = false;

    public ServiceAnnotationPostProcessor(String... packagesToScan) {
        this(Arrays.asList(packagesToScan));
    }

    public <T> ServiceAnnotationPostProcessor(Collection<String> packagesToScan) {
        this(new LinkedHashSet<>(packagesToScan));
    }

    public ServiceAnnotationPostProcessor(Set<String> packagesToScan) {
        this.packagesToScan = packagesToScan;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.resolvedPackagesToScan = resolvePackagesToScan(packagesToScan);
    }

    /**
     * 解析要扫描的包，主要是去除包名为空的包
     *
     * @param packagesToScan 要扫描的包
     * @return 要扫描的包
     */
    private Set<String> resolvePackagesToScan(Set<String> packagesToScan) {
        Set<String> resolvedPackagesToScan = new LinkedHashSet<>(packagesToScan.size());
        for (String packageToScan : packagesToScan) {
            if (packageToScan != null && packageToScan.trim().length() != 0) {
                String resolvedPackageToScan = environment.resolvePlaceholders(packageToScan.trim());
                resolvedPackagesToScan.add(resolvedPackageToScan);
            }
        }
        return resolvedPackagesToScan;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(@NotNull BeanDefinitionRegistry registry) throws BeansException {
        this.registry = registry;
        scanServiceBeans(resolvedPackagesToScan, registry);
    }

    /**
     * Scan and registers service beans whose classes was annotated {@link NaiveRpcService}
     *
     * @param packagesToScan The base packages to scan
     * @param registry       {@link BeanDefinitionRegistry}
     */
    private void scanServiceBeans(Set<String> packagesToScan, BeanDefinitionRegistry registry) {
        scanned = true;
        if (packagesToScan == null || packagesToScan.isEmpty()) {
            if (log.isWarnEnabled()) {
                log.warn("packagesToScan is empty , ServiceBean registry will be ignored!");
            }
            return;
        }

        NaiveRpcClassPathBeanDefinitionScanner scanner =
                new NaiveRpcClassPathBeanDefinitionScanner(registry, environment, resourceLoader);
        BeanNameGenerator beanNameGenerator = resolveBeanNameGenerator(registry);
        scanner.setBeanNameGenerator(beanNameGenerator);
        for (Class<? extends Annotation> annotationType : serviceAnnotationTypes) {
            scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));
        }
        // scanner.addExcludeFilter();

        for (String packageToScan : packagesToScan) {
            // avoid duplicated scans
            if (servicePackagesHolder.isPackageScanned(packageToScan)) {
                if (log.isInfoEnabled()) {
                    log.info("Ignore package who has already bean scanned: " + packageToScan);
                }
                continue;
            }
            // Registers @NaiveRpcService Bean first
            scanner.scan(packageToScan);
            // Finds all BeanDefinitionHolders of @NaiveRpcService whether @ComponentScan scans or not.
            Set<BeanDefinitionHolder> beanDefinitionHolders =
                    findServiceBeanDefinitionHolders(scanner, packageToScan, registry, beanNameGenerator);
            if (!CollectionUtils.isEmpty(beanDefinitionHolders)) {
                if (log.isInfoEnabled()) {
                    List<String> serviceClasses = new ArrayList<>(beanDefinitionHolders.size());
                    for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                        serviceClasses.add(beanDefinitionHolder.getBeanDefinition().getBeanClassName());
                    }
                    log.info("Found " + beanDefinitionHolders.size()
                            + " classes annotated by naive-rpc @NaiveRpcService under package ["
                            + packageToScan + "]: " + serviceClasses);
                }

                for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                    // 处理扫描后的Bean，将bean注册到register中
                    processScannedBeanDefinition(beanDefinitionHolder);
                    servicePackagesHolder.addScannedClass(
                            beanDefinitionHolder.getBeanDefinition().getBeanClassName());
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("No class annotated by naive-rpc @Service was found under package [" + packageToScan + "]");
                }
            }
            servicePackagesHolder.addScannedPackage(packageToScan);
        }
    }

    private BeanNameGenerator resolveBeanNameGenerator(BeanDefinitionRegistry registry) {
        BeanNameGenerator beanNameGenerator = null;
        if (registry instanceof SingletonBeanRegistry) {
            SingletonBeanRegistry singletonBeanRegistry = (SingletonBeanRegistry) registry;
            beanNameGenerator = (BeanNameGenerator) singletonBeanRegistry.getSingleton(
                    AnnotationConfigUtils.CONFIGURATION_BEAN_NAME_GENERATOR);
        }
        if (beanNameGenerator == null) {
            if (log.isInfoEnabled()) {
                log.info("BeanNameGenerator bean can't be found in BeanFactory with name ["
                        + AnnotationConfigUtils.CONFIGURATION_BEAN_NAME_GENERATOR + "]");
                log.info("BeanNameGenerator will be a instance of " +
                        AnnotationBeanNameGenerator.class.getName() +
                        " , it maybe a potential problem on bean name generation.");
            }
            beanNameGenerator = new AnnotationBeanNameGenerator();
        }
        return beanNameGenerator;
    }

    private Set<BeanDefinitionHolder> findServiceBeanDefinitionHolders(ClassPathBeanDefinitionScanner scanner,
                                                                       String packageToScan,
                                                                       BeanDefinitionRegistry registry,
                                                                       BeanNameGenerator beanNameGenerator) {
        Set<BeanDefinition> beanDefinitions = scanner.findCandidateComponents(packageToScan);
        Set<BeanDefinitionHolder> beanDefinitionHolders = new LinkedHashSet<>(beanDefinitions.size());
        for (BeanDefinition beanDefinition : beanDefinitions) {
            String beanName = beanNameGenerator.generateBeanName(beanDefinition, registry);
            BeanDefinitionHolder beanDefinitionHolder = new BeanDefinitionHolder(beanDefinition, beanName);
            beanDefinitionHolders.add(beanDefinitionHolder);
        }
        return beanDefinitionHolders;
    }

    private void processScannedBeanDefinition(BeanDefinitionHolder beanDefinitionHolder) {
        Class<?> beanClass = resolveClass(beanDefinitionHolder);
        Annotation service = findServiceAnnotation(beanClass);
        // The attributes of @NaiveRpcService annotation
        Map<String, Object> serviceAnnotationAttributes = AnnotationUtils.getAnnotationAttributes(service);
        String serviceInterface = ServiceAnnotationUtils.resolveInterfaceName(serviceAnnotationAttributes, beanClass);
        String annotatedServiceBeanName = beanDefinitionHolder.getBeanName();
        // ServiceBean Bean name
        String beanName = generateServiceBeanName(serviceAnnotationAttributes, serviceInterface);
        //  AbstractBeanDefinition serviceBeanDefinition = buildServiceBeanDefinition(serviceAnnotationAttributes, serviceInterface, annotatedServiceBeanName);
        BeanDefinition serviceBeanDefinition = beanDefinitionHolder.getBeanDefinition();
        registerServiceBeanDefinition(beanName, serviceBeanDefinition, serviceInterface);
    }

    private Class<?> resolveClass(BeanDefinitionHolder beanDefinitionHolder) {
        BeanDefinition beanDefinition = beanDefinitionHolder.getBeanDefinition();
        return resolveClass(beanDefinition);
    }

    private Class<?> resolveClass(BeanDefinition beanDefinition) {
        String beanClassName = beanDefinition.getBeanClassName();
        return ClassUtils.resolveClassName(Objects.requireNonNull(beanClassName), classLoader);
    }

    private Annotation findServiceAnnotation(Class<?> beanClass) {
        return serviceAnnotationTypes
                .stream()
                .map(annotationType -> AnnotatedElementUtils.findMergedAnnotation(beanClass, annotationType))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Generates the bean name
     *
     * @param serviceAnnotationAttributes serviceAnnotationAttributes
     * @param serviceInterface            the class of interface annotated {@link NaiveRpcService}
     * @return bean name
     */
    private String generateServiceBeanName(Map<String, Object> serviceAnnotationAttributes, String serviceInterface) {
        ServiceBeanNameBuilder builder = ServiceBeanNameBuilder.create(serviceInterface, environment);
        return builder.build();
    }

    private void registerServiceBeanDefinition(String beanName,
                                               BeanDefinition serviceBeanDefinition,
                                               String serviceInterface) {
        if (registry.containsBeanDefinition(beanName)) {
            BeanDefinition existingDefinition = registry.getBeanDefinition(beanName);
            if (existingDefinition.equals(serviceBeanDefinition)) {
                return;
            }
            String msg = "Found duplicated BeanDefinition of service interface [" + serviceInterface + "] " +
                    "with bean name [" + beanName + "], existing definition [ " + existingDefinition + "], " +
                    "new definition [" + serviceBeanDefinition + "]";
            log.error(msg);
            throw new BeanDefinitionStoreException(serviceBeanDefinition.getResourceDescription(), beanName, msg);
        }

        registry.registerBeanDefinition(beanName, serviceBeanDefinition);
        if (log.isInfoEnabled()) {
            log.info("Register ServiceBean[" + beanName + "]: " + serviceBeanDefinition);
        }
    }

    @Override
    public void postProcessBeanFactory(@NotNull ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (this.registry == null) {
            this.registry = (BeanDefinitionRegistry) beanFactory;
        }

        // scan bean definitions
        String[] beanNames = beanFactory.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            Map<String, Object> annotationAttributes = getServiceAnnotationAttributes(beanDefinition);
            if (annotationAttributes != null) {
                // process @NaiveRpcService at java-config @bean method
                processAnnotatedBeanDefinition(
                        beanName, (AnnotatedBeanDefinition) beanDefinition, annotationAttributes);
            }
        }

        if (!scanned) {
            // In spring 3.x, may be not call postProcessBeanDefinitionRegistry(), so scan service class here
            scanServiceBeans(resolvedPackagesToScan, registry);
        }
    }

    private Map<String, Object> getServiceAnnotationAttributes(BeanDefinition beanDefinition) {
        if (beanDefinition instanceof AnnotatedBeanDefinition) {
            AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition) beanDefinition;
            MethodMetadata factoryMethodMetadata = annotatedBeanDefinition.getFactoryMethodMetadata();
            if (factoryMethodMetadata != null) {
                for (Class<? extends Annotation> annotationType : serviceAnnotationTypes) {
                    if (factoryMethodMetadata.isAnnotated(annotationType.getName())) {
                        Map<String, Object> annotationAttributes =
                                factoryMethodMetadata.getAnnotationAttributes(annotationType.getName());
                        return ServiceAnnotationUtils.filterDefaultValues(annotationType,
                                Objects.requireNonNull(annotationAttributes));
                    }
                }
            }
        }
        return null;
    }

    private void processAnnotatedBeanDefinition(String beanName,
                                                AnnotatedBeanDefinition beanDefinition,
                                                Map<String, Object> annotationAttributes) {
        Map<String, Object> serviceAnnotationAttributes = new LinkedHashMap<>(annotationAttributes);
        // get bean class from return type
        MethodMetadata methodMetadata = Objects.requireNonNull(beanDefinition.getFactoryMethodMetadata());
        String returnTypeName = methodMetadata.getReturnTypeName();
        Class<?> beanClass = ClassUtils.resolveClassName(returnTypeName, classLoader);
        String serviceInterface = ServiceAnnotationUtils.resolveInterfaceName(
                serviceAnnotationAttributes, beanClass);
        // ServiceBean Bean name
        String serviceBeanName = generateServiceBeanName(serviceAnnotationAttributes, serviceInterface);
        registerServiceBeanDefinition(serviceBeanName, beanDefinition, serviceInterface);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.servicePackagesHolder = applicationContext.getBean(
                ServicePackagesHolder.BEAN_NAME, ServicePackagesHolder.class);
    }

    @Override
    public void setEnvironment(@NotNull Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(@NotNull ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setBeanClassLoader(@NotNull ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

}