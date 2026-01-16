/*
 * Copyright(c) 2016 - Heliosphere Corp.
 * ---------------------------------------------------------------------------
 * This file is part of the Heliosphere's project which is licensed under the 
 * Apache license version 2 and use is subject to license terms.
 * You should have received a copy of the license with the project's artifact
 * binaries and/or sources.
 * 
 * License can be consulted at http://www.apache.org/licenses/LICENSE-2.0
 * ---------------------------------------------------------------------------
 */
package com.heliosphere.demeter.base.resource.bundle;

/**
 * Enumeration of the resource bundle externalized string of the {@code demeter-base} module. Each enumerated value maps to a key in the corresponding
 * resource bundle file.
 * <p>
 * <b>Note:</b><br>
 * In the Heliosphere's framework, the enumerated values are not in full uppercase.
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Christophe Resse - Heliosphere</a>
 * @version 1.0.0
 */
@SuppressWarnings("nls")
@BundleEnumRegister(priority = 50)
public enum BundleDemeterBase implements IBundle
{
	/**
	 * Bundle file name.
	 * <p>
	 * This enumerated value should always be created and must point to the name of the resource bundle so that the {@link ResourceBundleManager} can
	 * load and register and load the resource bundle file of the {@link BundleBase} class.
	 * <p>
	 * <b>Example</b>:<br>
	 * If the module's name is {@code foo.module}, then this enumerated value should have the value: {@code bundle.foo.module}
	 * <p>
	 * <b>Important</b>:<br>
	 * DO NOT DELETE THIS ENTRY!
	 */
	BundleFilename("bundle.demeter-base"),

	/**
	 * Root value for all messages of the {@code BundleBase} component.
	 * <p>
	 * <b>Note:</b><br>
	 * Must have the name of the project artifact followed by the dot (.) character. <b>Example</b>:<br>
	 * If the module's name is {@code foo.module}, then this enumerated value should have the value: {@code foo.module}
	 * <p>
	 * <b>Important</b>:<br>
	 * DO NOT DELETE THIS ENTRY!
	 */
	BundleRoot("demeter-base."),

	/*
	 * ---------- Test.* ----------
	 */

	/**
	 * A dummy message from component: drake-base.
	 */
	TestDummy("test.dummy"),

	/*
	 * ---------- Enumeration.* ----------
	 */

	/**
	 * Cannot create an enumerated value.
	 */
	CannotCreateEnumerated("enumeration.cannotCreateEnumerated"),

	/*
	 * ---------- Class.* ----------
	 */

	/**
	 * The given class is initializing.
	 */
	ClassInitializing("class.initializing"),

	/**
	 * The given class has initialized successfully.
	 */
	ClassInitialized("class.initialized"),

	/**
	 * The given class failed to initialized.
	 */
	ClassInitializationError("class.initializationError"),

	/*
	 * ---------- Resource.Bundle.* ----------
	 */

	/**
	 * The test resource bundle key that does not exists in the resource bundle file.
	 */
	TestResourceBundleKeyDoesNotExist("resource.bundle.keyDoesNotExist"),

	/**
	 * The given resource bundle cannot be found.
	 */
	ResourceBundleNotFound("resource.bundle.notFound"),

	/**
	 * An error occured with the given resource bundle.
	 */
	ResourceBundleError("resource.bundle.error"),

	/**
	 * The given resource bundle has been registered.
	 */
	ResourceBundleRegistered("resource.bundle.registered"),

	/**
	 * The given resource bundle has been replaced.
	 */
	ResourceBundleReplaced("resource.bundle.replaced"),

	/**
	 * The given resource bundle is already registered.
	 */
	ResourceBundleAlreadyRegistered("resource.bundle.alreadyRegistered"),

	/**
	 * The given resource bundle file name cannot be null or empty.
	 */
	ResourceBundleInvalidName("resource.bundle.invalidName"),

	/**
	 * The given resource bundle key cannot be null or empty.
	 */
	ResourceBundleInvalidKey("resource.bundle.invalidKey"),

	/**
	 * The given resource bundle locale cannot be null.
	 */
	ResourceBundleInvalidLocale("resource.bundle.invalidLocale"),

	/**
	 * Resource bundle configuration file cannot be null or empty
	 */
	ResourceBundleInvalidConfiguration("resource.bundle.invalidConfiguration"),

	/*
	 * ---------- Encoding.* ----------
	 */

	/**
	 * Error thrown when trying to register in the class catalog a null class.
	 */
	EncodingClassToRegisterIsNull("encoding.classToRegisterIsNull"),

	/**
	 * Error thrown when trying to encode/decode some data and no class catalog has been set.
	 */
	EncodingClassCatalogIsNull("encoding.classCatalogIsNull"),

	/**
	 * Error thrown when trying to encode/decode some data and the object codec is null.
	 */
	EncodingCodecObjectIsNull("encoding.codecObjectIsNull"),

	/**
	 * Error thrown when trying to encode/decode some data and the data encoding algorithm is null.
	 */
	EncodingAlgorithmIsNull("encoding.algorithmIsNull"),

	/**
	 * Error thrown when trying to encode some data and the object passed as parameter is null.
	 */
	EncodingObjectIsNull("encoding.objectIsNull"),

	/**
	 * Error thrown when trying to encode/decode some data and the buffer passed as parameter is null.
	 */
	EncodingBufferIsNull("encoding.bufferIsNull"),

	/**
	 * Message indicating that a class is already registered in the class catalog.
	 */
	EncodingClassAlreadyRegistered("encoding.classAlreadyRegistered"),

	/**
	 * Error thrown while decoding an element for which there is a mismatch on the serial version uid.
	 * <p>
	 * <code>
	 * Cannot encode/decode data for [type=field, name={0}, class={1}]. Expected
	 * version of objects do not match.
	 * </code>
	 */
	EncodingSerialVersionUidMismatch("encoding.serialVersionUidMismatch"),

	/**
	 * Message indicating that a class does not implement the {@code Serializable} interace.
	 */
	EncodingNotImplementingSerializable("encoding.notImplementingSerializable"),

	/**
	 * Message indicating that an error occurs while encoding or decoding data.
	 */
	EncodingError("encoding.error"),

	/**
	 * Message indicating that a class to be registered is in collision with a class already registered in the class catalog.
	 */
	EncodingCollision("encoding.collision"),

	/**
	 * Message indicating that a class is registered by the class catalog.
	 */
	EncodingClassRegistered("encoding.classRegistered"),

	/**
	 * Message indicating that a class is unregistered from the class catalog.
	 */
	EncodingClassUnregistered("encoding.classUnregistered"),

	/**
	 * Message indicating that a class has not been found in the class catalog.
	 */
	EncodingClassNotFound("encoding.classNotFound"),

	/**
	 * Message indicating that a class has not been found in the class catalog.
	 */
	EncodingClassNotFoundId("encoding.classNotFoundId"),

	/**
	 * Error thrown while decoding an element for which the declaring class does not implement the {@code Encodable} interface.
	 * <p>
	 * <code>
	 * Element [name={0}, class={1}] has an invalid (not supported) class. To be
	 * encodable and/or decodable, the class must implement the Encodable
	 * interface.
	 * </code>
	 */
	UnsupportedClassType("encoding.unsupportedClassType"),

	/**
	 * Error thrown while decoding a primitive element that is not recognized as a primitive data type.
	 * <p>
	 * <code>
	 * Element [name={0}, class={1}] has an invalid (not supported) primitive
	 * data type.
	 * </code>
	 */
	UnsupportedPrimitiveType("encoding.unsupportedPrimitiveType"),

	/**
	 * Element is declared as transient, and thus will be excluded from the encoding/decoding process.
	 * <p>
	 * <code>
	 * Element [type=field, name={0}] is excluded of the encoding/decoding
	 * process because it is declared as transient.
	 * </code>
	 */
	TransientAttribute("encoding.transientAttribute"),

	/**
	 * No class catalog has been defined or class catalog not found.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Encoding.NoClassCatalogFound}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code No class catalog found}
	 */
	NoClassCatalogFound("encoding.noClassCatalogFound"),

	/*
	 * ---------- Properties.* ----------
	 */

	/**
	 * The given properties cannot be null.
	 */
	PropertiesCannotBeNull("properties.cannotBeNull"),

	/**
	 * The given property has not been found.
	 */
	PropertyNotFound("properties.propertyNotFound"),

	/**
	 * The given property has been loaded successfully.
	 */
	PropertyLoaded("properties.propertyLoaded"),

	/**
	 * The given properties is initializing.
	 */
	PropertiesInitialization("properties.initialization"),

	/**
	 * The given properties is initialized successfully.
	 */
	PropertiesInitialized("properties.initialized"),

	/**
	 * The given properties initialization failed.
	 */
	PropertiesInitializationFailed("properties.initializationFailed"),

	/*
	 * ---------- Command.* ----------
	 */

	/**
	 * Command registered.
	 * <p>
	 * {@code Command [name= 0}, class={1}, status=registered]}
	 */
	CommandRegistered("command.registered"),

	/**
	 * Command type is not registered.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.TypeNotRegistered}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command [command.type= 0}, command.protocol.class={1}] is not part of the registered command protocol}
	 */
	CommandTypeNotRegistered("command.typeNotRegistered"),

	/**
	 * A command converter has been registered.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.converterRegistered}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>Command converter registered [converter.name= {0}, converter.fullname={1}]</dd>
	 * <dt><b>Param 0:</b></dt>
	 * <dd>Converter class name</dd>
	 * <dt><b>Param 1:</b></dt>
	 * <dd>Converter class fullname</dd>
	 * </dl>
	 */
	CommandConverterRegistered("command.converterRegistered"),

	/**
	 * A command converter has been unregistered.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.converterUnregistered}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>Command converter unregistered [converter.name= {0}, converter.fullname={1}]</dd>
	 * <dt><b>Param 0:</b></dt>
	 * <dd>Converter class name</dd>
	 * <dt><b>Param 1:</b></dt>
	 * <dd>Converter class fullname</dd>
	 * </dl>
	 */
	CommandConverterUnregistered("command.converterUnregistered"),

	/**
	 * The given input command converter cannot be null.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.inputConverterCannotBeNull}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>Input command converter cannot be null</dd>
	 * </dl>
	 */
	CommandInputConverterCannotBeNull("command.inputConverterCannotBeNull"),

	/**
	 * The given command handler cannot be null.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.handlerCannotBeNull}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>Command handler cannot be null</dd>
	 * </dl>
	 */
	CommandHandlerCannotBeNull("command.handlerCannotBeNull"),

	/**
	 * A command must be annotated with the {@code @Command} annotation.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.annotationRequired}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>A command must be annotated with the @Command annotation</dd>
	 * </dl>
	 */
	CommandAnnotationRequired("command.annotationRequired"),

	/**
	 * The given command method cannot be null.
	 * <hr>
	 * <dl>
	 * <dt><b>Bundle key:</b></dt>
	 * <dd>{@code command.methodCannotBeNull}</dd>
	 * <dt><b>Message:</b></dt>
	 * <dd>Command method cannot be null</dd>
	 * </dl>
	 */
	CommandMethodCannotBeNull("command.methodCannotBeNull"),

	/**
	 * Cannot find a command class for the given textual command.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.ClassNotFoundForTextual}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Cannot find a command class corresponding to [command= 0}]}
	 */
	CommandClassNotFoundForTextual("command.classNotFoundForTextual"),

	/**
	 * Command does not match the regular expression.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.DoesNotMatchRegularExpression}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command [command.class= 0}, command.value={1}, command.regex={2}, command.usage={3}] does not match its regular expression}
	 */
	CommandDoesNotMatchRegularExpression("command.doesNotMatchRegularExpression"),

	/**
	 * Command alias is already registered with another command.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.AliasAlreadyRegistered}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command alias [command.alias= 0}, command.name={1}] is already registered with another command}
	 */
	CommandAliasAlreadyRegistered("command.aliasAlreadyRegistered"),

	/**
	 * Command name is already registered.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.AlreadyRegistered}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command [command.name= 0}] is already registered}
	 */
	CommandAlreadyRegistered("command.alreadyRegistered"),

	/**
	 * Command is excluded from registration.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.ExcludedFromRegistration}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command [command.category= 0}, command.type={1}] is excluded from registration}
	 */
	CommandExcludedFromRegistration("command.excludedFromRegistration"),

	/**
	 * Command type cannot be null.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.TypeCannotBeNull}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command type cannot be null}
	 */
	CommandTypeCannotBeNull("command.typeCannotBeNull"),

	/**
	 * Command key cannot be null.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.KeyCannotBeNull}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command key cannot be null}
	 */
	CommandKeyCannotBeNull("command.keyCannotBeNull"),

	/**
	 * Command category cannot be null.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.CategoryCannotBeNull}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Command category cannot be null}
	 */
	CommandCategoryCannotBeNull("command.categoryCannotBeNull"),

	/**
	 * Textual command representation cannot be null.
	 * <p>
	 * <b>Key:</b><br>
	 * {@code Command.TextualCannotBeNull}
	 * <p>
	 * <b>Message:</b><br>
	 * {@code Textual command representation cannot be null}
	 */
	CommandTextualCannotBeNull("command.textualCannotBeNull"),

	/*
	 * ---------- Terminal.* ----------
	 */

	/**
	 * A terminal and its associated thread has been created.
	 */
	TerminalCreated("terminal.created"),

	/**
	 * A terminal has been started.
	 */
	TerminalStarted("terminal.started"),

	/**
	 * A terminal has been stopped.
	 */
	TerminalStopped("terminal.stopped"),

	/**
	 * A terminal has been suspended.
	 */
	TerminalSuspended("terminal.suspended"),

	/**
	 * A terminal has been resumed.
	 */
	TerminalResumed("terminal.resumed"),

	/**
	 * A terminal listener has been registered.
	 */
	TerminalListenerRegistered("terminal.listenerRegistered"),

	/**
	 * A terminal listener has been unregistered.
	 */
	TerminalListenerUnregistered("terminal.listenerUnregistered"),

	/**
	 * A command entered on the terminal is notified to a listener.
	 */
	TerminalListenerNotification("terminal.listenerNotification"),

	/*
	 * ---------- Message.* ----------
	 */

	/**
	 * An error occured while decoding a message.
	 */
	MessageDecodingError("message.decodingError"),

	/**
	 * An error occured while encoding a message.
	 */
	MessageEncodingError("message.encodingError"),

	/**
	 * Unknown message.
	 */
	MessageUnknown("message.unknown"),

	/**
	 * Message cannot be sent.
	 */
	MessageCannotBeSent("message.cannotBeSent"),

	/**
	 * A message has been received.
	 */
	MessageReceived("message.received"),

	/*
	 * ---------- PropertyApplication.* ----------
	 */

	/**
	 * Property containing the name of the application.
	 */
	PropertyApplicationName("property.application.name"),

	/**
	 * Property containing the version of the application.
	 */
	PropertyApplicationVersion("property.application.version"),

	/**
	 * Property containing the copyright of the application.
	 */
	PropertyApplicationCopyright("property.application.copyright"),

	/**
	 * Property containing the locale to be used by the application.
	 */
	PropertyApplicationLocale("property.application.locale"),

	/**
	 * Property containing the path of the folder that contains the application log files.
	 */
	PropertyApplicationDirectoryLog("property.application.directory.log"),

	/**
	 * Property containing the path of the folder that contains the application data files.
	 */
	PropertyApplicationDirectoryData("property.application.directory.data"),

	/**
	 * Property defining the message encoder type to use to encode/decode messages.
	 */
	PropertyApplicationMessageEncoderType("property.application.message.encoder.type"),

	/**
	 * Property defining the message protocol class name.
	 */
	PropertyApplicationMessageProtocolClassname("property.application.message.protocol.classname"),

	/**
	 * Property defining the data encoder algorithm enumeration class name.
	 */
	PropertyApplicationDataEncoderAlgorithmClassname("property.application.data.encoder.algorithm.classname"),

	/**
	 * Property defining the data encoder algorithm type.
	 */
	PropertyApplicationDataEncoderAlgorithmType("property.application.data.encoder.algorithm.type");

	/**
	 * Resource bundle key.
	 */
	private final String key;

	/**
	 * Creates a new enumerated value based on the resource bundle key.
	 * <p>
	 * @param key Resource bundle key.
	 */
	private BundleDemeterBase(final String key)
	{
		this.key = key;
	}

	@Override
	public final String getKey()
	{
		if (!key.equals(BundleDemeterBase.BundleRoot.key) && !key.equals(BundleDemeterBase.BundleFilename.key))
		{
			return BundleDemeterBase.BundleRoot.getKey() + key;
		}

		return key;
	}

	@Override
	public final String getValue()
	{
		return ResourceBundleManager.getMessage(this);
	}
}