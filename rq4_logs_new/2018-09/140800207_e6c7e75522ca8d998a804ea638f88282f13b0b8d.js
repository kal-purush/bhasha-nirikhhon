const { registerBlockType, createBlock } = wp.blocks,
  { PlainText, InspectorControls } = wp.editor,
  { Fragment } = wp.element,
  { SelectControl } = wp.components,
  { __ } = wp.i18n;

import logo from '../../icons/carrieforde_logo-color.svg';
import Icon from '../../components/icon';

const LANGUAGES = [
  { name: 'HTML', class: 'markup' },
  { name: 'CSS', class: 'css' },
  { name: 'JavaScript', class: 'javascript' },
  { name: 'Sass (SCSS)', class: 'scss' },
  { name: 'React (JSX)', class: 'jsx' },
  { name: 'Bash', class: 'bash' },
  { name: 'PHP', class: 'php' },
  { name: 'Handlebars', class: 'handlebars' },
  { name: 'JSON', class: 'json' }
];

/**
 * Every block starts by registering a new block type definition.
 * @see https://wordpress.org/gutenberg/handbook/block-api/
 */
registerBlockType('carrieforde-blocks/code', {
  title: __('Code'),
  icon: <Icon icon={logo} className="icon-logo" />,
  category: 'carrieforde-blocks',
  supports: {
    html: false
  },
  attributes: {
    language: {
      type: 'string',
      default: 'html'
    },
    code: {
      type: 'array',
      source: 'children',
      selector: '.wp-block-carrieforde-blocks-code__code'
    }
  },
  transforms: {
    from: [
      {
        type: 'block',
        blocks: ['core/code'],
        transform: content =>
          createBlock('carrieforde-blocks/code', { content })
      }
    ]
  },

  /**
   * The edit function describes the structure of your block in the context of the editor.
   * This represents what the editor will render when the block is used.
   * @see https://wordpress.org/gutenberg/handbook/block-edit-save/#edit
   *
   * @param {Object} [props] Properties passed from the editor.
   * @return {Element}       Element to render.
   */
  edit: ({ attributes, setAttributes, className }) => {
    const { language, code } = attributes,
      onCodeChange = code => setAttributes({ code }),
      onLanguageSelect = language => setAttributes({ language });

    return (
      <Fragment>
        <pre className={className}>
          <InspectorControls>
            <SelectControl
              label={__('Select a Language')}
              value={language}
              onChange={onLanguageSelect}
              options={LANGUAGES.map(language => ({
                value: language.class,
                label: language.name
              }))}
            />
          </InspectorControls>
          <PlainText
            tagName="code"
            className={`wp-block-carrieforde-blocks-code__code language-${language}`}
            value={code}
            onChange={onCodeChange}
          />
        </pre>
      </Fragment>
    );
  },

  /**
   * The save function defines the way in which the different attributes should be combined
   * into the final markup, which is then serialized by Gutenberg into `post_content`.
   * @see https://wordpress.org/gutenberg/handbook/block-edit-save/#save
   *
   * @return {Element}       Element to render.
   */
  save: ({ attributes }) => {
    const { code, language } = attributes;
    return (
      <pre className={`wp-block-carrieforde-blocks-code language-${language}`}>
        <code
          className={`wp-block-carrieforde-blocks-code__code language-${language}`}
        >
          {code}
        </code>
      </pre>
    );
  }
});