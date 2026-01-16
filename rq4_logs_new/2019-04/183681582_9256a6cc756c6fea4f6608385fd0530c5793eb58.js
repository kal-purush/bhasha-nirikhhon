Reveal.initialize({
    // Display controls in the bottom right corner
    controls: true,
    // Display a presentation progress bar
    progress: true,
    // If true; each slide will be pushed to the browser history
    history: true,
    // Loops the presentation, defaults to false
    loop: false,
    // Flags if mouse wheel navigation should be enabled
    mouseWheel: true,
    // Apply a 3D roll to links on hover
    rollingLinks: true,
    // UI style
    theme: Reveal.getQueryHash().theme || 'default',
    // Transition style
    transition: Reveal.getQueryHash().transition || 'default',
    // Optional libraries used to extend on reveal.js
    dependencies: [
        { src: 'https://revealjs.com/lib/js/classList.js',
            condition: function() { return !document.body.classList; } },
        // { src: 'https://revealjs.com/js/reveal.js', async: true},
        { src: 'https://revealjs.com/plugin/markdown/marked.js',
            condition: function() { return !!document.querySelector( '[data-markdown]' ); } },
        { src: 'https://revealjs.com/plugin/markdown/markdown.js',
            condition: function() { return !!document.querySelector( '[data-markdown]' ); } },
        { src: 'https://revealjs.com/plugin/highlight/highlight.js', async: true,
            callback: function() { hljs.initHighlightingOnLoad(); } },
        { src: 'https://revealjs.com/plugin/notes/notes.js' }
    ]
});