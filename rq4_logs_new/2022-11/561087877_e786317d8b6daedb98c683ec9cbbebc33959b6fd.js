export default {
    name: 'TheLightboxCmponent',

    props: ['item'],

    emits: ['closelightbox'],

    template: `
    <section class="lightbox">
    <img @click="closeLB" src="images/closeIcon.png" class="lightbox_close">
    <!-- hero image on the left, text on the right -->
    <!-- her image is a background image -->
    <article>
        <h3 class="lb_heading">Captain America - the First Avenger</h3>
        <p class="lb_text">
            Lorem ipsum dolor sit amet consectetur, adipisicing elit. Nostrum autem pariatur dicta eum dolorum nulla neque laborum praesentium quo possimus, mollitia, dolor omnis rerum dolorem ratione in natus? Nihil, mollitia!
        </p>
        <p class="lb_text">
            Lorem ipsum dolor sit amet consectetur adipisicing elit. Fuga quaerat totam, unde quam aspernatur quasi delectus sed earum nobis voluptate voluptas suscipit repudiandae explicabo placeat numquam error. Praesentium, quisquam illum.
        </p>
    </article>
</section>
    
    
    `,
    methods: {
        closeLB() {
            this.$emit('closelightbox');
        }
    },
}