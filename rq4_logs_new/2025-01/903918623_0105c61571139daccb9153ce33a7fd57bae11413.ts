import { NextResponse } from 'next/server';
import OpenAI from 'openai';

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Function to search Amazon for products
async function searchAmazonProducts(query: string, budget: string) {
  // Convert budget to price range
  let maxPrice;
  switch (budget) {
    case 'budget':
      maxPrice = 50;
      break;
    case 'medium':
      maxPrice = 150;
      break;
    case 'luxury':
      maxPrice = 500;
      break;
    default:
      maxPrice = 150;
  }

  try {
    // For now, we'll return a formatted search URL
    // In production, you would integrate with Amazon's Product Advertising API
    const searchQuery = encodeURIComponent(query);
    return {
      name: query,
      price: maxPrice,
      description: `${query} matching your style`,
      style_match: "Based on your Instagram inspiration",
      shop_link: `https://www.amazon.com/s?k=${searchQuery}&rh=p_36%3A-${maxPrice}00`
    };
  } catch (error) {
    console.error('Error searching Amazon:', error);
    return null;
  }
}

export async function POST(req: Request) {
  try {
    const { image, budget, preferences } = await req.json();

    if (!image) {
      return NextResponse.json(
        { error: 'No image provided' },
        { status: 400 }
      );
    }

    // Remove the data URL prefix to get just the base64 image data
    const base64Image = image.split(',')[1];

    // Improved prompt for detailed style analysis
    const stylePrompt = `Analyze this Instagram fashion image in detail and provide a structured analysis in the following JSON format:
{
  "overall_aesthetic": "Brief description of the overall style aesthetic",
  "key_pieces": [
    {
      "item": "Specific item name",
      "description": "Detailed description including cut, material, fit",
      "style_elements": "Key style elements that make it stand out"
    }
  ],
  "color_palette": {
    "primary": ["color1", "color2"],
    "accent": ["color3", "color4"]
  },
  "styling_patterns": [
    "Pattern 1: e.g., layering technique",
    "Pattern 2: e.g., color combination principle"
  ],
  "recommended_searches": [
    "Specific search term for similar items"
  ]
}

Focus on identifying specific, searchable items and their unique characteristics. Consider the budget level: ${budget}`;

    // Analyze the image using GPT-4o
    const response = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: stylePrompt
            },
            {
              type: "image_url",
              image_url: {
                url: `data:image/jpeg;base64,${base64Image}`
              }
            }
          ]
        }
      ],
      max_tokens: 1000,
    });

    // Parse the AI response
    let analysisText = response.choices[0].message.content || "{}";
    
    // Clean up the response text by removing markdown formatting
    analysisText = analysisText.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    
    // Parse the cleaned JSON
    const analysis = JSON.parse(analysisText);
    
    // Search for products based on the analysis
    const productPromises = analysis.recommended_searches.map(search => 
      searchAmazonProducts(search, budget)
    );
    
    const products = (await Promise.all(productPromises)).filter(p => p !== null);

    // Transform the analysis into our recommendation format
    const recommendations = [
      {
        type: "Core Style Elements",
        items: products,
        aesthetic: analysis.overall_aesthetic,
        color_palette: [...analysis.color_palette.primary, ...analysis.color_palette.accent]
      },
      {
        type: "Styling Tips",
        items: analysis.styling_patterns.map(pattern => ({
          name: "Styling Tip",
          description: pattern,
          style_match: "Based on analyzed patterns",
          price: 0
        })),
        aesthetic: "Styling Techniques",
        color_palette: analysis.color_palette.primary
      }
    ];

    return NextResponse.json({ recommendations });
  } catch (error) {
    console.error('Error analyzing style:', error);
    return NextResponse.json(
      { error: 'Failed to analyze style' },
      { status: 500 }
    );
  }
}