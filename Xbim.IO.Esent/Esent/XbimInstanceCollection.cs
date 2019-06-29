using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Xbim.Common;

namespace Xbim.IO.Esent
{
    /// <summary>
    /// A class providing access to a collection of in,stances in a model
    /// </summary>
    public class XbimInstanceCollection : IEntityCollection
    {
        protected readonly FilePersistedModel _model;
        // private readonly FilePersistedModel _model;
        
        public IEnumerable<IPersistEntity> OfType(string stringType, bool activate)
        {
            return _model.OfType(stringType, activate);
        }

        internal XbimInstanceCollection(FilePersistedModel esentModel)
        {
            _model = esentModel;
        }

        /// <summary>
        /// Returns the total number of Ifc Instances in this model
        /// </summary>
        public long Count { get { return _model.Count;  } }

        /// <summary>
        /// Returns the count of the number of instances of the specified type
        /// </summary>
        /// <typeparam name="TIfcType"></typeparam>
        /// <returns></returns>
        public long CountOf<TIfcType>() where TIfcType : IPersistEntity
        {
            return _model.CountOf<TIfcType>(); 
        }
        /// <summary>
        /// Returns all instances in the model of IfcType, IfcType may be an abstract Type
        /// </summary>
        /// <param name="activate">if true each instance is fullly populated from the database, if false population is deferred until the entity is activated</param>
        /// <returns></returns>
        public IEnumerable<TIfc> OfType<TIfc>(bool activate) where TIfc : IPersistEntity
        {
            return _model.OfType<TIfc>(activate);
        }


        public IEnumerable<T> Where<T>(Func<T, bool> condition, string inverseProperty, IPersistEntity inverseArgument) where T : IPersistEntity
        {
            return _model.Where(condition, inverseProperty, inverseArgument);
        }

        public T FirstOrDefault<T>() where T : IPersistEntity
        {
            return OfType<T>().FirstOrDefault();
        }

        public T FirstOrDefault<T>(Func<T, bool> expr) where T : IPersistEntity
        {
            return Where(expr).FirstOrDefault();
        }

        public T FirstOrDefault<T>(Func<T, bool> condition, string inverseProperty, IPersistEntity inverseArgument) where T : IPersistEntity
        {
            return Where(condition, inverseProperty, inverseArgument).FirstOrDefault();
        }

        public IEnumerable<TIfc> OfType<TIfc>() where TIfc : IPersistEntity
        {
            return _model.OfType<TIfc>();
        }

        //public IEnumerable<TIfcType> OfType<TIfcType>() where TIfcType : IPersistEntity
        //{
        //    return cache.OfType<TIfcType>(false);
        //}

        /// <summary>
        ///   Filters the Ifc Instances based on their Type and the predicate
        /// </summary>
        /// <typeparam name = "TIfcType">Ifc Type to filter</typeparam>
        /// <param name = "expression">function to execute</param>
        /// <returns></returns>
        public IEnumerable<TIfcType> Where<TIfcType>(Func<TIfcType, bool> expression) where TIfcType : IPersistEntity
        {
            return _model.Where(expression);
        }

        /// <summary>
        /// Returns an enumerabale of all the instance handles in the model
        /// </summary>
        public IEnumerable<XbimInstanceHandle> Handles()
        {
            return _model.InstanceHandles;
        }


        /// <summary>
        /// Returns an enumerable of all handles of the specified type in the model
        /// </summary>
        /// <typeparam name="T">The type of entity required</typeparam>
        /// <returns></returns>
        public IEnumerable<XbimInstanceHandle> Handles<T>()
        {
            return _model.InstanceHandlesOfType<T>();
        }

        /// <summary>
        /// Returns an instance from the Model with the corresponding label
        /// </summary>
        /// <param name="label">entity label to retrieve</param>
        /// <returns></returns>
        public IPersistEntity this[int label]
        {
            get
            {
                return _model.GetInstance(label, true, true);
            }
        }
       
        /// <summary>
        /// Returns the Ifc entity for a given Geometry Label
        /// </summary>
        /// <param name="geometryLabel"></param>
        /// <returns></returns>
        public IPersistEntity GetFromGeometryLabel(int geometryLabel)
        {
            var filledGeomData = _model.GetGeometryHandle(geometryLabel);
            return _model.GetInstance(filledGeomData.ProductLabel, true, true);
        }


        /// <summary>
        ///   Creates a new Ifc Persistent Instance, this is an undoable operation
        /// </summary>
        /// <typeparam name = "TIfcType"> The Ifc Type, this cannot be an abstract class. An exception will be thrown if the type is not a valid Ifc Type  </typeparam>
        public TIfcType New<TIfcType>() where TIfcType : IInstantiableEntity
        {
            var t = typeof(TIfcType);
            var e = (TIfcType)New(t);
            return e;
        }
        /// <summary>
        ///   Creates and Instance of TIfcType and initializes the properties in accordance with the lambda expression
        ///   i.e. Person person = CreateInstance&gt;Person&lt;(p =&lt; { p.FamilyName = "Undefined"; p.GivenName = "Joe"; });
        /// </summary>
        /// <typeparam name = "TIfcType"></typeparam>
        /// <param name = "initPropertiesFunc"></param>
        /// <returns></returns>
        public TIfcType New<TIfcType>(Action<TIfcType> initPropertiesFunc) where TIfcType : IInstantiableEntity
        {
            var instance = New<TIfcType>();
            if (initPropertiesFunc != null)
                initPropertiesFunc(instance);
            return instance;
        }

        /// <summary>
        /// Creates and returns a new instance of Type t, sets the label to the specificed value.
        /// This is a reversabel operation
        /// 
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public IPersistEntity New(Type t)
        {
            var entity = _model.CreateNew(t);
            _model.HandleEntityChange(ChangeType.New, entity, 0);
            return entity;

        }

       
        /// <summary>
        /// Returns true if the instance label is in the current model, 
        /// Use with care, does not check that the instance is in the current model, only the label exists
        /// </summary>
        /// <param name="entityLabel"></param>
        /// <returns></returns>
        public  bool Contains(int entityLabel)
        {
            return _model.Contains(entityLabel);
        }

        /// <summary>
        /// Returns true if the instance is in the current model
        /// </summary>
        /// <param name="instance"></param>
        /// <returns></returns>
        public  bool Contains(IPersistEntity instance)
        {
            return _model.Contains(instance);
        }

     

        IEnumerator<IPersistEntity> IEnumerable<IPersistEntity>.GetEnumerator()
        {
            return new XbimInstancesEntityEnumerator(_model);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new XbimInstancesEntityEnumerator(_model);
        }
    }

    public delegate void InitNewEntity(IPersistEntity entity);

}
