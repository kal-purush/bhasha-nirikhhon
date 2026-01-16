import { copy, emptyDir } from 'https://deno.land/std@0.78.0/fs/mod.ts';
import { rollup } from 'https://deno.land/x/drollup@2.58.0+0.20.0/mod.ts';

// function render({ tem, out }: { tem: string; out: string }, option: { [key: string]: any }) {
//   // ...
// }

async function requestBundle({ inp, out }: { inp: { urn: string }; out: { urn: string } }): Promise<void> {
  const options = {
    input: inp.urn,
    output: {
      file: out.urn,
      format: 'es' as const,
      sourcemap: true,
    },
  };

  const bundle = await rollup(options);
  await bundle.write(options.output);
  await bundle.close();
}

async function requestModule({ out }: { out: { urn: string } }): Promise<void> {
  await Promise.all([
    requestBundle({
      inp: { urn: './packages/dev-inspect-content/lib/inspect-te-inline.ts' },
      out: { urn: `${out.urn}/assets/inspect-te-inline.js` },
    }),
    requestBundle({
      inp: { urn: './packages/dev-inspect-content/lib/inspect-te.ts' },
      out: { urn: `${out.urn}/assets/inspect-te.js` },
    }),
  ]);
}

async function requestAssets({ out }: { out: { urn: string } }): Promise<void> {
  const inp = { urn: './packages/dev-inspect-content/page' };

  for await (const nod of Deno.readDir(inp.urn)) //
    await copy(`${inp.urn}/${nod.name}`, `${out.urn}/${nod.name}`);
}

{
  const out = { urn: './docs/.pagelet/gh-pages' };

  await emptyDir(`${out.urn}`);
  await requestAssets({ out });
  await requestModule({ out });
}

{
  const out = { urn: './docs/.pagelet/gh-production' };

  await emptyDir(`${out.urn}`);
  await requestAssets({ out });
  await requestModule({ out });
}