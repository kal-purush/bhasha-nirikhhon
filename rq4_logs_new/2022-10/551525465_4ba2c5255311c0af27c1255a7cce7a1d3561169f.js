function ContentComponent({ headline, text }) {
  return (
    <section>
      <h2>{headline}</h2>
      <p class="text-content">{text}</p>
    </section>
  );
}
export default ContentComponent;