using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[Serializable]
public class EventCard
{
    public string interactionType;
    public string requestText;
    public Response yesResponse;
    public Response noResponse;
}

[Serializable]
public class Response
{
    public int gold;
    public int kingdomMentalHealth;
    public string responseText;
}