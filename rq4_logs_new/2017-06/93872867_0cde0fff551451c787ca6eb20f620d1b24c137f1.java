/*
 * Copyright 2017 Software Engineering and Synthesis Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sesygroup.choreography.concreteparticipantbehavior.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.sesygroup.choreography.concreteparticipantbehavior.model.action.AsynchronousReceiveActionTransition;
import com.sesygroup.choreography.concreteparticipantbehavior.model.action.AsynchronousSendActionTransition;
import com.sesygroup.choreography.concreteparticipantbehavior.model.action.SynchronousReceiveActionTransition;
import com.sesygroup.choreography.concreteparticipantbehavior.model.action.SynchronousSendActionTransition;

/**
 *
 * @author Alexander Perucci (http://www.alexanderperucci.com/)
 *
 */
public class ConcreteParticipantBehavior implements Serializable {
   private static final long serialVersionUID = 8815617060540221440L;
   private Set<State> states;
   private State initialState;
   private Set<Message> messages;
   private Set<Transition> transitions;

   public ConcreteParticipantBehavior() {
      super();
      states = new HashSet<State>();
      initialState = null;
      messages = new HashSet<Message>();
      transitions = new HashSet<Transition>();
   }

   public ConcreteParticipantBehavior(final Set<State> states, final State initialState, final Set<Message> messages,
         final Set<Transition> transitions) {
      super();
      this.states = states;
      this.initialState = initialState;
      this.messages = messages;
      this.transitions = transitions;
   }

   public Set<State> getStates() {
      return states;
   }

   public void setStates(final Set<State> states) {
      this.states = states;
   }

   public State getInitialState() {
      return initialState;
   }

   public void setInitialState(final State initialState) {
      this.initialState = initialState;
   }

   public Set<Message> getMessages() {
      return messages;
   }

   public void setMessages(final Set<Message> messages) {
      this.messages = messages;
   }

   public Set<Transition> getTransitions() {
      return transitions;
   }

   public void setTransitions(final Set<Transition> transitions) {
      this.transitions = transitions;
   }

   /**
    * Checks whether the SourceState and TargetState of the transition are contained in the set of states. Checks
    * whether the transition is an instance of synchronous or asynchronous SendAction then the OutputMessage should be
    * in the set of messages. Checks whether the transition is an instance of synchronous or asynchronous ReceiveAction
    * then the InputMessage should be in the set of messages. Checks whether the initialState is contained in the set of
    * states.
    *
    * @return {@code true} if the ConcreteParticipantBehavior is validated, {@code false} otherwise
    */
   public boolean validate() {
      // TODO improve validate method

      // checks whether the ConcreteParticipantBehavior has an empty set of states, messages, transitions and the
      // initialState is equals to null
      if (initialState == null && states.isEmpty() && messages.isEmpty() && transitions.isEmpty()) {
         return true;
      }

      for (Transition transition : transitions) {
         // checks whether the SourceState and TargetState of the transition are contained in the set of states
         if (!states.contains(transition.getSourceState()) || !states.contains(transition.getTargetState())) {
            return false;
         }
         // checks whether the transition is an instance of synchronous or asynchronous SendAction then the
         // OutputMessage should be in the set of
         // messages
         if ((transition instanceof SynchronousSendActionTransition
               || transition instanceof AsynchronousSendActionTransition)
               && !messages.contains(((SynchronousSendActionTransition) transition).getOutputMessage())) {
            return false;
         }
         // checks whether the transition is an instance of synchronous or asynchronous ReceiveAction then the
         // InputMessage should be in the set of
         // messages
         if ((transition instanceof SynchronousReceiveActionTransition
               || transition instanceof AsynchronousReceiveActionTransition)
               && !messages.contains(((AsynchronousReceiveActionTransition) transition).getInputMessage())) {
            return false;
         }
         // we do not check whether the transition is an instance of InternalAction because this action not contains a
         // message
      }

      // checks whether the initialState is contained in the set of states
      if ((initialState != null && states.isEmpty()) || (initialState != null && !states.contains(initialState))
            || (initialState == null && !states.isEmpty())) {
         // TODO throw new ValidationException("The initialState is not contained in the set of states");
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((initialState == null)
            ? 0
            : initialState.hashCode());
      result = prime * result + ((messages == null)
            ? 0
            : messages.hashCode());
      result = prime * result + ((states == null)
            ? 0
            : states.hashCode());
      result = prime * result + ((transitions == null)
            ? 0
            : transitions.hashCode());
      return result;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (getClass() != obj.getClass()) {
         return false;
      }
      ConcreteParticipantBehavior other = (ConcreteParticipantBehavior) obj;
      if (initialState == null) {
         if (other.initialState != null) {
            return false;
         }
      } else if (!initialState.equals(other.initialState)) {
         return false;
      }
      if (messages == null) {
         if (other.messages != null) {
            return false;
         }
      } else if (!messages.equals(other.messages)) {
         return false;
      }
      if (states == null) {
         if (other.states != null) {
            return false;
         }
      } else if (!states.equals(other.states)) {
         return false;
      }
      if (transitions == null) {
         if (other.transitions != null) {
            return false;
         }
      } else if (!transitions.equals(other.transitions)) {
         return false;
      }
      return true;
   }
}